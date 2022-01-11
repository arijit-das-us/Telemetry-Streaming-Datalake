// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Cubic usage
// MAGIC 1. read messages from Kafka topic intelsatusage
// MAGIC 2. get the NetworkProfile->ID and join with sspc table by rowkey to get the usage Name
// MAGIC 3. get the terminal ID and join terminal table by rowkey to get sat router ID
// MAGIC 4. join router table by rowkey and sat router ID
// MAGIC 5. get the model serial number
// MAGIC 6. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 7. join metricmapping table by primaryRawSymbol and mappingType by the usage->byteReceived and byteTransmitted and usage type (USAGE_NMS, USAGE_MGMT, USAGE_DATA)
// MAGIC 8. get the Kymeta metric Id from above join from metric mapping table
// MAGIC 9. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to delta table
// MAGIC 9. stream the data (remote ID, Metric ID, provider ID, Timestamp. value) to SQL Warehouse

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFCubic = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFCubic.printSchema


// COMMAND ----------

val streamingDfCubic = streamingInputDFCubic
.select(explode(from_json(get_json_object(($"value").cast("string"), "$.data"),ArrayType(StringType))).alias("data"),$"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)
.withColumn("timestamp",get_json_object(($"data"), "$.SessionEndDateTime"))
.withColumn("timestamp", unix_timestamp(substring($"timestamp",0,19), "yyyy-MM-dd HH:mm:ss"))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
.withColumn("Hour", from_unixtime($"timestamp","HH"))
.withColumn("SessionId",get_json_object(($"data"), "$.SessionId"))
.withColumn("MCCMNC",get_json_object(($"data"), "$.MCCMNC"))
.withColumn("ICCID",get_json_object(($"data"), "$.ICCID"))
.withColumn("MSISDN",get_json_object(($"data"), "$.MSISDN"))
.withColumn("MBUsed",get_json_object(($"data"), "$.MBUsed"))
.withColumn("MBCharged",get_json_object(($"data"), "$.MBCharged"))
.withColumn("CallStatus",get_json_object(($"data"), "$.CallStatus"))
.withColumn("APNName",get_json_object(($"data"), "$.APNName"))
.withColumn("MCC",get_json_object(($"data"), "$.MCC"))
.withColumn("MNC",get_json_object(($"data"), "$.MNC"))

// COMMAND ----------

//display(streamingDfCubic)

// COMMAND ----------

val CubicWithHKStream = streamingDfCubic.withColumn("uniqueHashKey",sha1(concat(lit("cubicUsageForMBCharge"),$"ICCID",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "CubicPool1")


CubicWithHKStream
.withWatermark("TimestampYYMMSSHHMMSS", "48 hours")
.dropDuplicates("uniqueHashKey")
.select("SessionId","MCCMNC","ICCID","MSISDN","MBUsed","MBCharged","CallStatus","APNName","MCC","MNC","timestamp","Datestamp", "Hour","EnqueuedTime", "LoadTime")
.writeStream
.queryName("CubicQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Cubic-kafka2bronze")
.option("path",basePath + "DeltaTable/Cubic-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("120 seconds")).start()

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFCubicBronze = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Cubic-bronze")

// COMMAND ----------

val streamingDFCubicBronzeMBCharge = streamingInputDFCubicBronze
.withColumn("MBCharged",round($"MBCharged".cast(DoubleType)*1024*1024).cast(LongType).cast(StringType))

// COMMAND ----------

val CubicJoinMetricMappingDF = streamingDFCubicBronzeMBCharge
.withColumnRenamed("ICCID","serialnumber")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )
.withColumn("metric",lit("MBCharged"))
.withColumn("metricId", lit(9003).cast(LongType))
.withColumn("metricProviderID", lit(4).cast(LongType))
.withColumnRenamed("MBCharged","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumn("element", lit(null).cast(StringType))
.withColumn("deviceType", lit("DEV_SIM"))
.withColumn("model", lit(null).cast(StringType))
.filter($"value" =!= "0")


// COMMAND ----------

//display(CubicJoinMetricMappingDF)
//all data with current time stamp >= 1603139720 are valid. 

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "CubicPool2")

val CubicExplodedQuery =  
CubicJoinMetricMappingDF
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("CubicQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Cubic-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("120 seconds")).start()