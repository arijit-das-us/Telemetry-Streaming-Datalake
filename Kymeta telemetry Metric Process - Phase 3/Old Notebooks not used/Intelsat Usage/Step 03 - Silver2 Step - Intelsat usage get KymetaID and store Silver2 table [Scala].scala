// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Intelsat Usage
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

import org.apache.spark.sql.functions._

val streamingInputDFIntelsatUsage = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/IntelsatUsage-silver-step1")
//streamingInputDFIntelsatUsage.printSchema
//display(streamingInputDFIntelsatUsage)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

//display(factSSPC)

// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with sspc to get the usage name

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//val IntelsatUsageWithWaterMark = streamingInputDFIntelsatUsage
//.withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//.withWatermark("TimestampYYMMSSHHMMSS", "336 hours")

val IntelsatUsageWithSSPCUsage = streamingInputDFIntelsatUsage.join(factSSPC).where(streamingInputDFIntelsatUsage("SSPCId") === factSSPC("RowKey_SSPC")).select("terminalId", "SSPCId" , "timestamp", "bytesReceived", "bytesTransmitted", "Datestamp","PartitionKey_SSPC", "UsageName")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


// COMMAND ----------

// MAGIC %md 
// MAGIC join the streaming data from Intelsat with Terminal and then to router

// COMMAND ----------


val IntelsatUsageWithTerminal = IntelsatUsageWithSSPCUsage.join(factTerminal).where(IntelsatUsageWithSSPCUsage("terminalId") === factTerminal("RowKey")).select("terminalId", "SSPCId" , "unix_timestamp", "bytesReceived", "bytesTransmitted", "Datestamp", "UsageName", "coremoduleid").withColumnRenamed("coremoduleid", "SatRouterID")


val IntelsatUsageWithRouterID = IntelsatUsageWithTerminal.join(factRouter).where(IntelsatUsageWithTerminal("SatRouterID") === factRouter("RowKey")).select("terminalId", "SSPCId" , "unix_timestamp", "bytesReceived", "bytesTransmitted", "Datestamp", "UsageName", "SatRouterID", "serialnumber","model")
//IntelsatUsageWithRouterID.printSchema

// COMMAND ----------

val IntelsatUsageJoinMetricMappingRxTx = IntelsatUsageWithRouterID.join(
    metricmappingsDF,
    expr(""" 
      UsageName = mappingType and (rawSymbol = 'bytesReceived' or rawSymbol = 'bytesTransmitted')
      """
    )
  )
.withColumn("value", when(col("rawSymbol") === "bytesReceived",col("bytesReceived").cast(StringType)).when(col("rawSymbol") === "bytesTransmitted",col("bytesTransmitted").cast(StringType)))
.withColumnRenamed("rawSymbol","metric")
.select("terminalId", "SSPCId", "unix_timestamp", "metric", "value", "Datestamp", "serialnumber", "model", "metricId", "metricProviderId")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("SSPCId", "element")
.withColumn("deviceType", lit("DEV_MODEM"))
//IntelsatUsageJoinMetricMappingRxTx.printSchema

// COMMAND ----------

//IntelsatUsageJoinMetricMappingRxTx.printSchema

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val IntelsatUsageExplodedQuery =  
IntelsatUsageJoinMetricMappingRxTx
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/IntelsatUsage-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()