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
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Cubic-Batch-silver-step2")

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

 dfWithRemoteID
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.write
.format("delta")
.partitionBy("Datestamp")
//.option("mergeSchema", "true")
.mode("append")
.save(basePath + "DeltaTable/Metric-gold-raw")



// COMMAND ----------

// MAGIC %md 
// MAGIC #Custom Metrics