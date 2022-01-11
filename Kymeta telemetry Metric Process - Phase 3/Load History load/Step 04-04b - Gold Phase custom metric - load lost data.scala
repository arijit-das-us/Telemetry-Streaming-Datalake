// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Step 04
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

//spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
//spark.conf.set("spark.sql.streaming.aggregation.stateFormatVersion", 1)

// COMMAND ----------

// MAGIC %md 
// MAGIC #Custom Metrics

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val factCustommetrics = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/custommetrics")

//display(factCustommetrics)

// COMMAND ----------

val streamingInputDFGold = 
spark.read
  .format("delta")
 // .option("startingVersion", "1199578")
  //.option("startingTimestamp", "2020-10-01")
  .load(basePath + "DeltaTable/Metric-gold-raw")
 // .filter($"metricProviderId" === 3)
  .filter($"dateStamp"==="2021-07-20" or $"dateStamp"==="2021-07-21" or $"dateStamp"==="2021-07-22")
 // .filter($"unixTimestamp".isNull())

// COMMAND ----------

display(streamingInputDFGold)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfcustomMetricSUM = streamingInputDFGold.join(factCustommetrics).where(streamingInputDFGold("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "SUM")) 
.select("unixTimestamp","dateStamp", "valueInDouble", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType","currentTimestamp")
.withColumn("TimestampYYMMddHHmmss", from_unixtime($"currentTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

val dfCustomMetricsAggSumUp = dfcustomMetricSUM
.withWatermark("TimestampYYMMddHHmmss", "60 seconds")
.groupBy($"unixTimestamp",$"RemoteId", $"metricProviderId", $"categoryId", $"metricId", $"TimestampYYMMddHHmmss")
.agg((sum($"valueInDouble")).cast(DecimalType(30,15)).alias("valueInDouble"))
//dfIntelsatUsageAddup.printSchema

// COMMAND ----------

val dfCustomMetricStreamSUM = dfCustomMetricsAggSumUp
.withColumn("valueInString", lit(null).cast(StringType))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")
.withColumn("Datestamp", from_unixtime($"unixTimestamp","yyyy-MM-dd"))
.withColumn("currentTimestamp", unix_timestamp())
.withColumn("aggregationType", lit("SUM"))


// COMMAND ----------

/*
val dfCustomMetricsSumNormalized = dfCustomMetricStreamSUM
.withColumn("valueInDouble",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, (col("valueInDouble") * 1000).cast(DecimalType(30,15))).otherwise(col("valueInDouble")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, 9003).otherwise(col("kymetaMetricId")))
       */

// COMMAND ----------


val dfCustomMetricsSumNormalized = dfCustomMetricStreamSUM
.withColumn("valueInDouble",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, (col("valueInDouble") * 1000).cast(DecimalType(30,15))).otherwise(col("valueInDouble")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, 9003).otherwise(col("kymetaMetricId")))
.withColumn("valueInDouble",
       when((col("kymetaMetricId") === 9008 || col("kymetaMetricId") === 9009) && col("metricProviderID") === 3, ((col("valueInDouble")/60) * 1000).cast(DecimalType(30,15))).when((col("kymetaMetricId") === 9008 || col("kymetaMetricId") === 9009) && (col("metricProviderID") === 1|| col("metricProviderID") === 2), (col("valueInDouble")/300).cast(DecimalType(30,15))).otherwise(col("valueInDouble")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9008, 9006).otherwise(col("kymetaMetricId")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9009, 9007).otherwise(col("kymetaMetricId")))

// COMMAND ----------

display(dfCustomMetricsSumNormalized)

// COMMAND ----------

val streamingInputDFCustomSUM = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-custom-sum")
  .filter($"dateStamp"==="2021-07-20" or $"dateStamp"==="2021-07-21" or $"dateStamp"==="2021-07-22")

// COMMAND ----------

println(streamingInputDFCustomSUM.count())

// COMMAND ----------

println(dfCustomMetricsSumNormalized.count())

// COMMAND ----------

val dfDiff = dfCustomMetricsSumNormalized
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType")
.except(streamingInputDFCustomSUM.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType"))

// COMMAND ----------

println(dfDiff.count())

// COMMAND ----------

dfDiff
.write
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.mode("overwrite")
.save(basePath + "DeltaTable/testHistory/Metric-gold-custom-sum-0723")  

// COMMAND ----------

val streamingInputDFCustomSUM = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/testHistory/Metric-gold-custom-sum-0723")
  .withColumn("currentTimestamp", unix_timestamp())

// COMMAND ----------

display(streamingInputDFCustomSUM)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldCustomPool1_HL")

val dfExplodedQuery2 =  
streamingInputDFCustomSUM
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.writeStream
.queryName("GoldCustomQuery1_HL")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/HistoryLoad/metric-custom-sum-072321")
.option("path",basePath + "DeltaTable/Metric-gold-custom-sum")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

