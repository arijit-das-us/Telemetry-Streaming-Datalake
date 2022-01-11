// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Hub Stats
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

val streamingInputDFHubStats = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Hubstats-bronze")
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))
//streamingInputDFHubStats.printSchema
//display(streamingInputDFHubStats)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val HubStatsWithHashKeyStream = streamingInputDFHubStats.withColumn("uniqueHashKey",sha1(concat(lit("hubstats"),$"element",$"metric",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

//IntelsatUsageWithHashKeyStream.printSchema


// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val HubStatsExplodedQuery =  
HubStatsWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric", "EnqueuedTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstats-silver-step1")
.option("path",basePath + "DeltaTable/Hubstats-silver-step1")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()