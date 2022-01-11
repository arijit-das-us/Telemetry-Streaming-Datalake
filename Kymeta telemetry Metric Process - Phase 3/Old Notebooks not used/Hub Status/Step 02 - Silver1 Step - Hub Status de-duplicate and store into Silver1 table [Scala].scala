// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Hub Status
// MAGIC 1. read messages from Kafka topic
// MAGIC 2. join with terminal table by rowkey
// MAGIC 3. join with router table by rowkey
// MAGIC 4. get the model serial number
// MAGIC 5. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 6. join metricmapping table by primaryRawSymbol
// MAGIC 7. join metrics table to get the Kymeta meric Id
// MAGIC 8. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to DB

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFHubStatus = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Hubstatus-bronze").filter($"timestamp" > (unix_timestamp() - lit(1209600)))
//streamingInputDFHubStatus.printSchema
//display(streamingInputDFHubStatus)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val HubStatusWithHashKeyStream = streamingInputDFHubStatus.withColumn("uniqueHashKey",sha1(concat(lit("hubstatus"),$"element_id",$"metric_id",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

//HubStatusWithHashKeyStream.printSchema


// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val HubStatusExplodedQuery =  
HubStatusWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("element_id","value","timestamp","Datestamp", "Hour", "metric_id", "EnqueuedTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstatus-silver-step1")
.option("path",basePath + "DeltaTable/Hubstatus-silver-step1")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("10 seconds")).start()