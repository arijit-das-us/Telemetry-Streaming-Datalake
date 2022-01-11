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

// load the stream into a dataframe for terminal table 
val factTerminal = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/terminal")
//display(factTerminal)

// COMMAND ----------

val factRouter = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/satelliterouter")
//display(factRouter)

// COMMAND ----------

val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings")
//display(metricmappingsDF)

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFHubStats = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Hubstats-silver-step1")
//streamingInputDFHubStats.printSchema
//display(streamingInputDFHubStats)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with sspc to get the usage name

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//val HubStatsWithWaterMark = streamingInputDFHubStats
//.withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//.withWatermark("TimestampYYMMSSHHMMSS", "336 hours")



// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with Terminal and then to router

// COMMAND ----------

val HubStatsWithTerminal = streamingInputDFHubStats.join(factTerminal).where(streamingInputDFHubStats("element") === factTerminal("RowKey"))
.select("element","mean_value","timestamp","Datestamp", "metric", "coremoduleid")
.withColumnRenamed("coremoduleid", "SatRouterID")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val HubStatsWithRouterID = HubStatsWithTerminal.join(factRouter).where(HubStatsWithTerminal("SatRouterID") === factRouter("RowKey")).select("element","mean_value","unix_timestamp","Datestamp",  "metric", "SatRouterID", "serialnumber","model")
//HubStatsWithRouterID.printSchema

// COMMAND ----------

val HubStatsJoinMetricMappingRxTx = HubStatsWithRouterID.join(
    metricmappingsDF,
    expr(""" 
      metric = rawSymbol
      """
    )
  )
.select("element","unix_timestamp","Datestamp", "mean_value","metric", "serialnumber","model","metricId", "metricProviderID")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("mean_value", "value")
.withColumn("deviceType", lit("DEV_MODEM"))

//HubStatsJoinMetricMappingRxTx.printSchema

// COMMAND ----------

//HubStatsJoinMetricMappingRxTx.printSchema

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val HubStatsExplodedQuery =  
HubStatsJoinMetricMappingRxTx
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstats-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()