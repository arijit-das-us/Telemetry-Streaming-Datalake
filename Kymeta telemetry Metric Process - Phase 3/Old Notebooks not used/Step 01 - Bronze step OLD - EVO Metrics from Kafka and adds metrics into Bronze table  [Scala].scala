// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - EVO Metrics
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

val streamingInputDFEvo = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
streamingInputDFEvo.printSchema


// COMMAND ----------

val streamingDfEvo = streamingInputDFEvo
.select(get_json_object(($"value").cast("string"), "$.header.table").alias("table"), explode(from_json(get_json_object(($"value").cast("string"), "$.data"),ArrayType(StringType))).alias("data"),$"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)
.withColumn("timestamp",get_json_object(($"data"), "$.timestamp"))
//.withColumn("timestamp",
//       when(length(col("timestamp")) === 2 , concat(lit("20"),col("timestamp"))).otherwise(col("timestamp")))
.withColumn("uniqueId",get_json_object(($"data"), "$.unique_id"))



// COMMAND ----------

//display(streamingDfEvo)

// COMMAND ----------

val EvoWithTableAndDatestamp = streamingDfEvo
.withColumn("timestamp", unix_timestamp($"timestamp", "MM/dd/yy hh:mm:ss aaa"))
.filter($"timestamp" > lit(1593561600))
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.withColumn("data",concat(lit("{\"header\": {\"table\":\""), $"table", lit("\"},\"data\":"),$"data", lit("}")))

// COMMAND ----------

//display(EvoWithTableAndDatestamp)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val EVOWithHashKeyStream = EvoWithTableAndDatestamp.withColumn("uniqueHashKey",sha1(concat(lit("EVO"),$"table",$"uniqueId", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val EVOExplodedQuery =  
EVOWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("table","uniqueId","timestamp","Datestamp", "Hour", "data", "EnqueuedTime", "LoadTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-bronze")
.option("path",basePath + "DeltaTable/EVO-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()

