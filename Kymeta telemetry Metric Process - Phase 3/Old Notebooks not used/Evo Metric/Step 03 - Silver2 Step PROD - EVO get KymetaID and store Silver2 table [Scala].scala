// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - EVO Metric
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

val streamingEVOSiver1 = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/EVO-silver-step1")
//streamingInputDFIntelsatUsage.printSchema


// COMMAND ----------

//display(streamingEVOSiver1)

// COMMAND ----------

val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings")
//metricmappingsDF.printSchema
//display(metricmappingsDF.filter($"metricProviderId" === 3))

// COMMAND ----------

val uniqueidmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmapping")
//display(uniqueidmappingsDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

val streamingEVOWithSerial = streamingEVOSiver1.join(uniqueidmappingsDF,$"uniqueId" === $"NetModemId")
//display(streamingEVOWithSerial)

// COMMAND ----------

val EVOJoinMetricMappingDF = streamingEVOWithSerial.join(
    metricmappingsDF,
    expr(""" 
       rawSymbol = Name and TableName = mappingType
      """
    )
  )
.select("ModemSn","TableName","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID")
.withColumnRenamed("ModemSn","serialnumber")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumnRenamed("TableName","element")
.withColumn("deviceType", lit("DEV_MODEM"))
.withColumn("model", lit(null).cast(StringType))

//display(EVOJoinMetricMappingDF)

// COMMAND ----------

val EVOAfterNormalize = EVOJoinMetricMappingDF
.withColumn("value",
       when(col("Kymeta_metricId") === 116 && col("metricProviderID") === 3, when(col("value") === "OK", "1").when(col("value") === "ALARM", "0").when(col("value") === "OFFLINE", "0").otherwise("2"))
      .otherwise(col("value")))
.withColumn("value",
       when(col("Kymeta_metricId") === 120 && col("metricProviderID") === 3, when(col("value") === "16", "SES-15_IG").when(col("value") === "5", "Echostar 105 IG").when(col("value") === "7", "HellasSAT_ME_IG").when(col("value") === "10", "HellasSAT_EU Beam").when(col("value") === "12", "Kumsan IG").otherwise("9999"))
      .otherwise(col("value")))

//display(HubStatusAfterNormalize)

// COMMAND ----------

val EVOWithHashKeyStream = EVOAfterNormalize.withColumn("uniqueHashKey",sha1(concat(lit("EVOSilver2"), $"unix_timestamp", $"Kymeta_metricId", $"metricProviderId", $"serialnumber"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unix_timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val EVOExplodedQuery =  
EVOWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "48 hours")
.dropDuplicates("uniqueHashKey")
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-silver-blob-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()