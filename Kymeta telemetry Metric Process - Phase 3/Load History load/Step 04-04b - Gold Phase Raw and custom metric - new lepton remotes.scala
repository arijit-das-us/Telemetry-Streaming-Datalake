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
  .load(basePath + "DeltaTable/testHistory/Metric-silver2-evo-newDevice")

// COMMAND ----------

display(streamingInputDFSilver)

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

display(dfWithRemoteID)

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
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/temp-checkpoint/metric-gold-raw-new-device")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFGoldRawEvo = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"valueInDouble" =!= 0 && ($"aggregationType" === "SUM" || $"aggregationType" === "AVG") && $"Datestamp" > "2021-03-20" && $"metricProviderId" === 3)
  .filter($"RemoteId".isin("6af42318-2dc7-4e75-9350-3c5e7a9dd693","e656ea05-b54b-4f0f-8edf-f04738928c05","5e62b6dd-bf3d-4d71-a38c-9895d4bfda48","fa20eb7b-a80c-4b4f-8866-836dadbf4468","0717b180-d218-413b-9b48-5a139b5add05","cf6c10ba-7d06-4c86-be8f-2bc68c671348","2aa9b942-15c3-4be7-a40c-9f1211fb37f0","68c1fb78-3613-4f39-94b7-520eddcca8be","820f5485-f4a7-486b-92e8-defb4b182162","6c7fa7d0-7eaf-4206-9869-b3647774d7d7","8abd2731-8cf4-4a1c-9cfc-252962611a6d","0c4fc0d6-f455-4a15-9250-176b8d8b6eb5","6e366f76-c93d-4a8e-b06a-552eff98d299","97bd92c1-5830-4928-aee2-23fce0b3b807","b3ddcf7c-5940-4063-8cc9-5a6ab58f7a8a","641911ff-c2a6-44b6-a544-9927774ad26f"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble","dateStamp")


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFMonth = InputDFGoldRawEvo
.withColumn("unixTimestamp", unix_timestamp(substring($"dateStamp",0,7),"yyyy-MM"))
.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val MetricAggMonthCosmos = aggregatesDFMonth
.withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

val configMapAggByMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByMonth = Config(configMapAggByMonth)

// COMMAND ----------

import org.apache.spark.sql.SaveMode
MetricAggMonthCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFDay = InputDFGoldRawEvo
.withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))
.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFHour = InputDFGoldRawEvo
.withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM-dd HH"),"yyyy-MM-dd HH"))
.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val MetricAggDayCosmos = aggregatesDFDay
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
  .select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

val configMapAggByDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByDay = Config(configMapAggByDay)


// COMMAND ----------

import org.apache.spark.sql.SaveMode
MetricAggDayCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByDay)

// COMMAND ----------

val MetricAggHourCosmos = aggregatesDFHour
.withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

val configMapAggByHour = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByHour,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByHour = Config(configMapAggByHour)

// COMMAND ----------

MetricAggHourCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)

// COMMAND ----------

val dfLatestMetric = spark.read
.format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"valueInDouble" =!= 0 && $"Datestamp" > "2021-03-20" && $"metricProviderId" === 3)
  .filter($"RemoteId".isin("6af42318-2dc7-4e75-9350-3c5e7a9dd693","e656ea05-b54b-4f0f-8edf-f04738928c05","5e62b6dd-bf3d-4d71-a38c-9895d4bfda48","fa20eb7b-a80c-4b4f-8866-836dadbf4468","0717b180-d218-413b-9b48-5a139b5add05","cf6c10ba-7d06-4c86-be8f-2bc68c671348","2aa9b942-15c3-4be7-a40c-9f1211fb37f0","68c1fb78-3613-4f39-94b7-520eddcca8be","820f5485-f4a7-486b-92e8-defb4b182162","6c7fa7d0-7eaf-4206-9869-b3647774d7d7","8abd2731-8cf4-4a1c-9cfc-252962611a6d","0c4fc0d6-f455-4a15-9250-176b8d8b6eb5","6e366f76-c93d-4a8e-b06a-552eff98d299","97bd92c1-5830-4928-aee2-23fce0b3b807","b3ddcf7c-5940-4063-8cc9-5a6ab58f7a8a","641911ff-c2a6-44b6-a544-9927774ad26f"))
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("unixTimestamp", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.coalesce(42)


// COMMAND ----------

//https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group/33878701

import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

val w3 = Window.partitionBy("id").orderBy(col("unixTimestamp").desc)
    
val dfTopByWindows = dfLatestMetric
.withColumn("row",row_number.over(w3))
.where($"row" === 1).drop("row")
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

val configMapAggLatest = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggLatest = Config(configMapAggLatest)

// COMMAND ----------

dfTopByWindows.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatest)