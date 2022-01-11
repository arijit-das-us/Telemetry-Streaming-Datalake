// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Step 04b
// MAGIC 1. read messages from Kafka topic intelsatusage
// MAGIC 2. get the NetworkProfile->ID and join with sspc table by rowkey to get the usage Name
// MAGIC 3. get the terminal ID and join terminal table by rowkey to get sat router ID
// MAGIC 4. join router table by rowkey and sat router ID
// MAGIC 5. get the model serial number
// MAGIC 6. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 7. join metricmapping table by primaryRawSymbol and mappingType by the usage->byteReceived and byteTransmitted and usage type (USAGE_NMS, USAGE_MGMT, USAGE_DATA)
// MAGIC 8. get the Kymeta metric Id from above join from metric mapping table
// MAGIC 9. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to delta table

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

val factCustommetrics = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/custommetrics")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
//streamingInputDF.printSchema
//display(streamingInputDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

val dfcustomMetricJOIN = streamingInputDF.join(factCustommetrics).where(streamingInputDF("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "JOIN")) 
.select("unixTimestamp","dateStamp", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType")
.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)


// COMMAND ----------

//display(dfcustomMetricJOIN)

// COMMAND ----------

val dfCustomMetricsAggJoin = dfcustomMetricJOIN
.withWatermark("TimestampYYMMddHHmmss", "300 seconds")
.groupBy($"unixTimestamp",$"dateStamp",  $"TimestampYYMMddHHmmss", $"metricProviderId", $"categoryId", $"RemoteId", $"metricId")
//.agg(expr("coalesce(first(concat(valueInDouble,'')),0)").alias("valueInString"))
//.agg(concat_ws(",", collect_list("kymetaMetricId"), lit(":"), collect_list("valueInDouble")) as "valueInString")
.agg(collect_list(struct("kymetaMetricId", "valueInString")).cast(StringType) as("valueInString"))

// COMMAND ----------

//dfCustomMetricsAggJoin.printSchema
//display(dfCustomMetricsAggSumUp)

// COMMAND ----------

display(dfCustomMetricsAggJoin)

// COMMAND ----------

val dfCustomMetricStreamSUM = dfCustomMetricsAggSumUp
.withColumn("valueInString", lit(null).cast(StringType))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")

val dfCustomMetricStreamJOIN = dfCustomMetricsAggJoin
.withColumn("valueInDouble", lit(null).cast(DecimalType(30,15)))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")

// COMMAND ----------

//dfCustomMetricStreamSUM.printSchema

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val dfExplodedQuery =  
dfCustomMetricStreamSUM
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.writeStream
.format("delta")
.partitionBy("dateStamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-custom-sum")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val dfExplodedQuery =  
dfCustomMetricStreamJOIN
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.writeStream
.format("delta")
.partitionBy("dateStamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-custom-join")
.option("path",basePath + "DeltaTable/Metric-gold-custom-join")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()