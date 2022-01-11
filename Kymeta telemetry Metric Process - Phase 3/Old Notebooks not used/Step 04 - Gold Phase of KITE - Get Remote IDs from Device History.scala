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
//display(factModem.filter($"serial" === "37958"))


// COMMAND ----------

val factRemote = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/remotes")
//factRemote.printSchema
//display(factRemote.filter($"Private" === true ))

// COMMAND ----------

val factProvider = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/metricproviders")

// COMMAND ----------

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")
//dfMetricAggType.printSchema()


// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDF = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-silver-step2")
//streamingInputDF.printSchema
//display(streamingInputDF)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Inner Join
// MAGIC 
// MAGIC Let's join these two data streams. This is exactly the same as joining two batch DataFrames/Datasets by RowKey

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//val dfWithWaterMark = streamingInputDF
//.withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unix_timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//.withWatermark("TimestampYYMMSSHHMMSS", "336 hours")

val modemHistoryDfUnixTS = factModem.withColumn("AddOnUnix_timestamp", unix_timestamp($"AddedOn")).withColumn("RemovedOnUnix_timestamp", unix_timestamp($"RemovedOn"))


// COMMAND ----------

//display(streamingInputDF.filter($"Kymeta_metricId" === 116 && $"element" === "25475"))

// COMMAND ----------

val dfWithRemoteID = streamingInputDF.join(modemHistoryDfUnixTS).where(streamingInputDF("serialnumber") === modemHistoryDfUnixTS("Serial") && streamingInputDF("deviceType") === modemHistoryDfUnixTS("Type") && ( streamingInputDF("unix_timestamp") >= modemHistoryDfUnixTS("AddOnUnix_timestamp")) && (modemHistoryDfUnixTS("RemovedOnUnix_timestamp").isNull || streamingInputDF("unix_timestamp") < modemHistoryDfUnixTS("RemovedOnUnix_timestamp")))
.join(dfMetricAggType).where(streamingInputDF("Kymeta_metricId") === dfMetricAggType("id"))
.join(factRemote).where(col("RemoteId") === factRemote("Id"))
.filter(col("isPrivate") === false  || (col("Private").isNull || col("Private") === false)  )
.withColumn("valueInDouble", when(col("aggregationType") === "SUM" or col("aggregationType") === "AVG",col("value").cast(DecimalType(30,15))))
.withColumn("valueInString", when(col("aggregationType") =!= "SUM" && col("aggregationType") =!= "AVG",col("value")))
.join(factProvider).where(streamingInputDF("metricProviderId") === factProvider("id"))
.select($"element" as "elementId", $"unix_timestamp".cast(LongType) as "unixTimestamp",$"Datestamp" as "dateStamp", $"metric", $"valueInDouble", $"valueInString", $"Kymeta_metricId" as "kymetaMetricId", $"metricProviderId", $"categoryId", $"RemoteId" as "remoteId")


//dfWithRemoteID.printSchema

// COMMAND ----------

//display(dfWithRemoteID.filter($"Kymeta_metricId" === 116  && $"element" === "25475"))

// COMMAND ----------

//val dfGolddeDup = dfWithRemoteID.withColumn("uniqueHashKey",sha1(concat(lit("raw"),$"remoteId",$"kymetaMetricId",$"metricProviderId",$"categoryId"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val dfExplodedQuery = 
dfWithRemoteID
// dfGolddeDup
//.withWatermark("TimestampYYMMSSHHMMSS", "30 seconds")
//.dropDuplicates("uniqueHashKey","TimestampYYMMSSHHMMSS")
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.writeStream
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-raw")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()