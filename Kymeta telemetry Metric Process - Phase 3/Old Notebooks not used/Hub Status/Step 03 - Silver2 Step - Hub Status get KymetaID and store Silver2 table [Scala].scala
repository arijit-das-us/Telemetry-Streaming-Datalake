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

val streamingInputDFHubStatus = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Hubstatus-silver-step1")
//streamingInputDFHubStatus.printSchema
//display(streamingInputDFHubStatus)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with sspc to get the usage name

// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with Terminal and then to router

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val HubStatusWithTerminal = streamingInputDFHubStatus.join(factTerminal).where(streamingInputDFHubStatus("element_id") === factTerminal("RowKey"))
.select("element_id","value","timestamp","Datestamp", "metric_id", "coremoduleid")
.withColumnRenamed("coremoduleid", "SatRouterID")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val HubStatusWithRouterID = HubStatusWithTerminal.join(factRouter).where(HubStatusWithTerminal("SatRouterID") === factRouter("RowKey")).select("element_id","value","unix_timestamp","Datestamp",  "metric_id", "SatRouterID", "serialnumber","model")
//display(HubStatusWithRouterID)

// COMMAND ----------

val HubStatusJoinMetricMappingRxTx = HubStatusWithRouterID.join(
    metricmappingsDF,
    expr(""" 
      metric_id = rawSymbol
      """
    )
  )
.select("element_id","unix_timestamp","Datestamp", "value","metric_id", "serialnumber","model","metricId", "metricProviderID")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("metric_id", "metric")
.withColumnRenamed("element_id", "element")
.withColumn("deviceType", lit("DEV_MODEM"))
//HubStatusJoinMetricMappingRxTx.printSchema
//display(HubStatusJoinMetricMappingRxTx)

// COMMAND ----------

val HubStatusAfterNormalize = HubStatusJoinMetricMappingRxTx
.withColumn("value",
       when(col("Kymeta_metricId") === 116 && col("metricProviderID") === 1, when(col("value") === "1", "1").when(col("value") === "6", "3").when((col("value") === "2" || col("value") === "3" || col("value") === "4" || col("value") === "5"), "0").otherwise("2"))
      .otherwise(col("value")))

//display(HubStatusAfterNormalize)

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val HubStatusExplodedQuery =  
HubStatusAfterNormalize
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstatus-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("15 seconds")).start()