// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Peplink Metric
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

val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 6)
//display(metricmappingsDF)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingPeplinkSiver1 = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Peplink-silver-step1")
//streamingInputDFIntelsatUsage.printSchema
//display(streamingPeplinkSiver1)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

val PeplinkJoinMetricMappingDF = streamingPeplinkSiver1.join(
    metricmappingsDF,
    expr(""" 
       rawSymbol = Name
      """
    )
  )
.select("serial","groupId","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID")
.withColumnRenamed("Serial","serialnumber")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumn("element", lit(null).cast(StringType))
.withColumn("deviceType", lit("DEV_MODEM"))
.withColumn("model", lit(null).cast(StringType))

// COMMAND ----------

//display(PeplinkJoinMetricMappingDF)
//PeplinkJoinMetricMappingDF.printSchema

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.

PeplinkJoinMetricMappingDF
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Peplink-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()