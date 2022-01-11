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

// load the stream into a dataframe for sspc table 
val factSSPC = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/sspc")
//display(factSSPC)

// COMMAND ----------

// load the stream into a dataframe for service plan table 
val factServicePlan = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/servicePlan")
//display(factServicePlan)

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFHubUsage = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Hubusage-silver-step1")
//streamingInputDFHubUsage.printSchema


// COMMAND ----------

//display(streamingInputDFHubUsage)

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
//val HubUsageWithWaterMark = streamingInputDFHubUsage
//.withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//.withWatermark("TimestampYYMMSSHHMMSS", "336 hours")



// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with SSPC and then to Service Plan

// COMMAND ----------

val HubUsageWithSSPC = streamingInputDFHubUsage.join(factSSPC).where(streamingInputDFHubUsage("element") === factSSPC("RowKey_SSPC")).select("element", "timestamp" , "mean_value", "metric", "Datestamp", "PartitionKey_SSPC", "UsageName")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val MetricUsageWithServicePlan = HubUsageWithSSPC.join(factServicePlan).where(HubUsageWithSSPC("PartitionKey_SSPC") === factServicePlan("RowKey_ServicePlan")).select("element","unix_timestamp","mean_value","metric", "Datestamp", "UsageName", "PartitionKey_ServicePlan")

//MetricUsageWithServicePlan.printSchema

// COMMAND ----------

//display(MetricUsageWithServicePlan)

// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with terminal and then to Router table

// COMMAND ----------

val HubUsageWithTerminal = MetricUsageWithServicePlan.join(factTerminal).where(MetricUsageWithServicePlan("PartitionKey_ServicePlan") === factTerminal("RowKey")).select("element","mean_value","unix_timestamp","Datestamp", "metric", "UsageName", "coremoduleid")
.withColumnRenamed("coremoduleid", "SatRouterID")


val HubUsageWithSatRouterID = HubUsageWithTerminal.join(factRouter).where(HubUsageWithTerminal("SatRouterID") === factRouter("RowKey")).select("element","mean_value","unix_timestamp","Datestamp", "metric", "UsageName", "SatRouterID", "serialnumber", "model")
//HubStatsWithRouterID.printSchema

// COMMAND ----------

//display(HubUsageWithSatRouterID)

// COMMAND ----------

val HubUsageJoinMetricMappingRxTx = HubUsageWithSatRouterID.join(
    metricmappingsDF,
    expr(""" 
      UsageName = mappingType and metric = rawSymbol 
      """
    )
  )
.select("element","unix_timestamp","Datestamp", "mean_value","metric", "serialnumber","model","metricId", "metricProviderID")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("mean_value", "value")
.withColumn("deviceType", lit("DEV_MODEM"))
//HubUsageJoinMetricMappingRxTx.printSchema

// COMMAND ----------

//HubUsageJoinMetricMappingRxTx.printSchema

// COMMAND ----------

//display(HubUsageJoinMetricMappingRxTx)

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val HubUsageExplodedQuery =  
HubUsageJoinMetricMappingRxTx
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubusage-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()