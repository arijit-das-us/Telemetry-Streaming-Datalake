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

val factModem = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/remotedevicehistory")
val factRemote = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/remotes")
val factProvider = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/metricproviders")
val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")


// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFSilver = 
spark.readStream
  .format("delta")
  .option("startingVersion", "1831951")
  .option("ignoreDeletes", "true")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-silver-step2")
 // .filter($"unix_timestamp" > lit(1635399644))

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val modemHistoryDfUnixTS = factModem.withColumn("AddOnUnix_timestamp", unix_timestamp($"AddedOn")).withColumn("RemovedOnUnix_timestamp", unix_timestamp($"RemovedOn"))


// COMMAND ----------

val dfWithRemoteID = streamingInputDFSilver.join(modemHistoryDfUnixTS).where(streamingInputDFSilver("serialnumber") === modemHistoryDfUnixTS("Serial") && streamingInputDFSilver("deviceType") === modemHistoryDfUnixTS("Type") && (streamingInputDFSilver("model").isNull || trim(streamingInputDFSilver("model")) === "" || streamingInputDFSilver("model") === modemHistoryDfUnixTS("Model")) && ( streamingInputDFSilver("unix_timestamp") >= modemHistoryDfUnixTS("AddOnUnix_timestamp")) && (modemHistoryDfUnixTS("RemovedOnUnix_timestamp").isNull || streamingInputDFSilver("unix_timestamp") < modemHistoryDfUnixTS("RemovedOnUnix_timestamp")))
.join(dfMetricAggType).where(streamingInputDFSilver("Kymeta_metricId") === dfMetricAggType("id"))
.join(factRemote).where(col("RemoteId") === factRemote("Id"))
.filter(col("isPrivate") === false  || (col("Private").isNull || col("Private") === false)  )
.withColumn("valueInDouble", when(col("aggregationType") === "SUM" or col("aggregationType") === "AVG",col("value").cast(DecimalType(30,15))))
.withColumn("valueInString", when(col("aggregationType") =!= "SUM" && col("aggregationType") =!= "AVG",col("value")))
.join(factProvider).where(streamingInputDFSilver("metricProviderId") === factProvider("id"))
.select($"element" as "elementId", $"unix_timestamp".cast(LongType) as "unixTimestamp",$"Datestamp" as "dateStamp", $"metric", $"valueInDouble", $"valueInString", $"Kymeta_metricId" as "kymetaMetricId", $"metricProviderId", $"categoryId", $"RemoteId" as "remoteId", $"aggregationType")
.withColumn("currentTimestamp", unix_timestamp())


//dfWithRemoteID.printSchema

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldPool1")

val dfExplodedQuery1 = 
dfWithRemoteID
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.writeStream
.queryName("GoldQuery1")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-raw-new-102821")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

// MAGIC %md 
// MAGIC #Serial Indexed ASM

// COMMAND ----------

val streamingInputDFSilverASM = 
spark.readStream
  .format("delta")
  .option("startingVersion", "1831675")
  .option("ignoreDeletes", "true")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-silver-step2")

// COMMAND ----------


val dfASMSerialIndexed = streamingInputDFSilverASM
.filter($"metricProviderId" === 5)
.join(dfMetricAggType).where(streamingInputDFSilverASM("Kymeta_metricId") === dfMetricAggType("id"))
.withColumn("valueInDouble", when(col("aggregationType") === "SUM" or col("aggregationType") === "AVG",col("value").cast(DecimalType(30,15))))
.withColumn("valueInString", when(col("aggregationType") =!= "SUM" && col("aggregationType") =!= "AVG",col("value")))
.join(factProvider).where(streamingInputDFSilverASM("metricProviderId") === factProvider("id"))
.select($"element" as "elementId", $"unix_timestamp".cast(LongType) as "unixTimestamp",$"Datestamp" as "dateStamp", $"metric", $"valueInDouble", $"valueInString", $"Kymeta_metricId" as "kymetaMetricId", $"metricProviderId", $"categoryId", $"serialnumber" as "remoteId", $"aggregationType")
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------


//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldPoolASM2")

val dfExplodedQuery4 = 
dfASMSerialIndexed
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.writeStream
.queryName("GoldQuery2")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-raw-serial-asm-102821")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()


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
spark.readStream
  .format("delta")
  .option("startingVersion", "1400237")
  //.option("startingTimestamp", "2020-10-01")
  .option("ignoreDeletes", "true")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-raw")

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

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldCustomPool1")

val dfExplodedQuery2 =  
dfCustomMetricsSumNormalized
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.writeStream
.queryName("GoldCustomQuery1")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-custom-sum-102821")
.option("path",basePath + "DeltaTable/Metric-gold-custom-sum")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

val streamingInputDFCustomSUM = 
spark.readStream
  .format("delta")
  .option("startingVersion", "771696")
  .option("ignoreDeletes", "true")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-custom-sum")

// COMMAND ----------

//unique hash key on "unixTimestamp", "remoteId", "metricProviderId", "kymetaMetricId", "categoryId"
val InputDFCustomSUMWithHKStream = streamingInputDFCustomSUM.withColumn("uniqueHashKey",sha1(concat(lit("CustomSumFor903"),$"remoteId",$"metricProviderId", $"kymetaMetricId",$"categoryId", $"unixTimestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldPool2")

val dfExplodedQuery3 = 
InputDFCustomSUMWithHKStream
.withWatermark("TimestampYYMMSSHHMMSS", "750 hours")
.dropDuplicates("uniqueHashKey")
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.writeStream
.queryName("GoldCustomQuery2")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/customsum-to-gold-raw-12182021")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("15 seconds")).start()