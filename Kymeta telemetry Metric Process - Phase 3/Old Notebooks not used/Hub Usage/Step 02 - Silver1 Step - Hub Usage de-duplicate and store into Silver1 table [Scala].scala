// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Hub Usage
// MAGIC 1. read messages from Kafka topic hubusage
// MAGIC 2. join with sspc table by rowkey
// MAGIC 3. join with terminalserviceplan table by rowkey
// MAGIC 4. join terminal table by rowkey
// MAGIC 5. join router table by rowkey
// MAGIC 6. get the model serial number
// MAGIC 7. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 8. join metricmapping table by primaryRawSymbol
// MAGIC 9. join metrics table and sspc table (there will be 3 metrics for usage, USAGE_NMS, USAGE_MGMT, USAGE_DATA, so you need the sspc name and hub metric id) to get the Kymeta metric Id
// MAGIC 10. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to DB

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFHubUsage = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Hubusage-bronze").filter($"timestamp" > (unix_timestamp() - lit(1209600)))
//streamingInputDFHubUsage.printSchema
//display(streamingInputDFHubUsage)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val HubUsageWithHashKeyStream = streamingInputDFHubUsage.withColumn("uniqueHashKey",sha1(concat(lit("hubusage"),$"element",$"metric",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

//HubUsageWithHashKeyStream.printSchema


// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val HubUsageExplodedQuery =  
HubUsageWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric", "EnqueuedTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubusage-silver-step1")
.option("path",basePath + "DeltaTable/Hubusage-silver-step1")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()