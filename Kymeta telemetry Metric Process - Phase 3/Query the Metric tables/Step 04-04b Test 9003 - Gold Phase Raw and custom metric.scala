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
  .load(basePath + "DeltaTable/Metric-silver-step2")

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

val dfWithRemoteID = streamingInputDFSilver.join(modemHistoryDfUnixTS).where(streamingInputDFSilver("serialnumber") === modemHistoryDfUnixTS("Serial") && streamingInputDFSilver("deviceType") === modemHistoryDfUnixTS("Type") && ( streamingInputDFSilver("unix_timestamp") >= modemHistoryDfUnixTS("AddOnUnix_timestamp")) && (modemHistoryDfUnixTS("RemovedOnUnix_timestamp").isNull || streamingInputDFSilver("unix_timestamp") < modemHistoryDfUnixTS("RemovedOnUnix_timestamp")))
.join(dfMetricAggType).where(streamingInputDFSilver("Kymeta_metricId") === dfMetricAggType("id"))
.join(factRemote).where(col("RemoteId") === factRemote("Id"))
.filter(col("isPrivate") === false  || (col("Private").isNull || col("Private") === false)  )
.withColumn("valueInDouble", when(col("aggregationType") === "SUM" or col("aggregationType") === "AVG",col("value").cast(DecimalType(30,15))))
.withColumn("valueInString", when(col("aggregationType") =!= "SUM" && col("aggregationType") =!= "AVG",col("value")))
.join(factProvider).where(streamingInputDFSilver("metricProviderId") === factProvider("id"))
.select($"element" as "elementId", $"unix_timestamp".cast(LongType) as "unixTimestamp",$"Datestamp" as "dateStamp", $"metric", $"valueInDouble", $"valueInString", $"Kymeta_metricId" as "kymetaMetricId", $"metricProviderId", $"categoryId", $"RemoteId" as "remoteId")
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
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "currentTimestamp")
.writeStream
.queryName("GoldQuery1")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-raw")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val factCustommetrics = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/custommetrics")

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY MetricGold

// COMMAND ----------

val streamingInputDFGold = 
spark.readStream
  .format("delta")
  //.option("startingVersion", "259339")
  .option("startingTimestamp", "2020-10-01")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter(col("metricProviderId") === 2)

// COMMAND ----------

display(streamingInputDFGold)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfcustomMetricSUM = streamingInputDFGold.join(factCustommetrics).where(streamingInputDFGold("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "SUM")) 
.select("unixTimestamp","dateStamp", "valueInDouble", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType","currentTimestamp")
.withColumn("TimestampYYMMddHHmmss", from_unixtime($"currentTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

//display(dfcustomMetricSUM.filter(col("metricProviderId") === 2))

// COMMAND ----------

val dfCustomMetricsAggSumUp = dfcustomMetricSUM
.withWatermark("TimestampYYMMddHHmmss", "600 seconds")
.groupBy($"unixTimestamp",$"RemoteId", $"metricProviderId", $"categoryId", $"metricId", $"TimestampYYMMddHHmmss")
.agg((sum($"valueInDouble")).cast(DecimalType(30,15)).alias("valueInDouble"))
//dfIntelsatUsageAddup.printSchema

// COMMAND ----------

//display(dfCustomMetricsAggSumUp.filter(col("metricProviderId") === 2 && col("metricId") === 9003))

// COMMAND ----------

val dfCustomMetricStreamSUM = dfCustomMetricsAggSumUp
.withColumn("valueInString", lit(null).cast(StringType))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")
.withColumn("Datestamp", from_unixtime($"unixTimestamp","yyyy-MM-dd"))
.withColumn("currentTimestamp", unix_timestamp())



// COMMAND ----------

val dfCustomMetricsSumNormalized = dfCustomMetricStreamSUM
.withColumn("valueInDouble",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, (col("valueInDouble") * 1024).cast(DecimalType(30,15))).otherwise(col("valueInDouble")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, 9003).otherwise(col("kymetaMetricId")))

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldCustomPool1")

val dfExplodedQuery2 =  
dfCustomMetricsSumNormalized
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "currentTimestamp")
.writeStream
.queryName("GoldCustomQuery1")
.format("delta")
.partitionBy("dateStamp")
.option("checkpointLocation", basePath + "DeltaTable/test9003/_checkpoint/metric-9003-sum-Oct1")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

val streamingInputDFGoldForJoin = 
spark.readStream
  .format("delta")
  .option("startingVersion", "259339")
  .load(basePath + "DeltaTable/Metric-gold-raw")

// COMMAND ----------

val dfcustomMetricJOIN = streamingInputDFGoldForJoin.join(factCustommetrics).where(streamingInputDFGoldForJoin("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "JOIN")) 
.select("unixTimestamp","dateStamp", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType")
.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)


// COMMAND ----------

val dfCustomMetricsAggJoin = dfcustomMetricJOIN
.withWatermark("TimestampYYMMddHHmmss", "300 seconds")
.groupBy($"unixTimestamp",$"dateStamp",  $"TimestampYYMMddHHmmss", $"metricProviderId", $"categoryId", $"RemoteId", $"metricId")
//.agg(expr("coalesce(first(concat(valueInDouble,'')),0)").alias("valueInString"))
//.agg(concat_ws(",", collect_list("kymetaMetricId"), lit(":"), collect_list("valueInDouble")) as "valueInString")
.agg(collect_list(struct("kymetaMetricId", "valueInString")).cast(StringType) as("valueInString"))

// COMMAND ----------

val dfCustomMetricStreamJOIN = dfCustomMetricsAggJoin
.withColumn("valueInDouble", lit(null).cast(DecimalType(30,15)))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")
.withColumn("currentTimestamp", unix_timestamp())

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldCustomPool2")

val dfExplodedQuery3 =  
dfCustomMetricStreamJOIN
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "currentTimestamp")
.writeStream
.queryName("GoldCustomQuery2")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-custom-join")
.option("path",basePath + "DeltaTable/Metric-gold-custom-join")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where dateStamp = '2020-10-01' and metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and currentTimestamp >= 1601571301 
// MAGIC order by unixTimestamp desc
// MAGIC --259339 : 2020-10-01T16:55:01.000+0000

// COMMAND ----------

val dfGold = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"metricProviderId" === 2 && ($"kymetaMetricId" === 71 || $"kymetaMetricId" === 72 || $"kymetaMetricId" === 84 || $"kymetaMetricId" === 85) && $"unixTimestamp">= 1601510400 && $"unixTimestamp" < 1601625600)

// COMMAND ----------

val factCustommetrics = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/custommetrics")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfcustomMetricSUM = dfGold.join(factCustommetrics).where(dfGold("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "SUM")) 
.select("unixTimestamp","dateStamp", "valueInDouble", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType")

// COMMAND ----------

val dfCustomMetricsAggSumUp = dfcustomMetricSUM
.groupBy($"unixTimestamp",$"RemoteId", $"metricProviderId", $"categoryId", $"metricId")
.agg((sum($"valueInDouble")).cast(DecimalType(30,15)).alias("valueInDouble"))
//dfIntelsatUsageAddup.printSchema

// COMMAND ----------

val dfCustomMetricSUM = dfCustomMetricsAggSumUp
.withColumn("valueInString", lit(null).cast(StringType))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")
.withColumn("Datestamp", from_unixtime($"unixTimestamp","yyyy-MM-dd"))
.withColumn("currentTimestamp", unix_timestamp())

// COMMAND ----------

dfCustomMetricSUM
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "currentTimestamp")
.write
.format("delta")
.partitionBy("dateStamp")
.mode("append")
.save(basePath + "DeltaTable/test9003/intelsat-9003-July-Sept")  

// COMMAND ----------

dfCustomMetricSUM
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "currentTimestamp")
.write
.format("delta")
.partitionBy("dateStamp")
.mode("append")
.save(basePath + "DeltaTable/Metric-gold-raw")  

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val Metric9003 = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/test9003/intelsat-9003-July-Sept")
  .filter($"unixTimestamp"< 1601510400)
  .withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
  .select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
  
//  .filter($"Datestamp" >= (date_sub(current_timestamp(), 30) cast StringType))

// COMMAND ----------

println(Metric9003.count())

// COMMAND ----------

// Configure the connection to your collection in Cosmos DB.
// Please refer to https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references
// for the description of the available configurations.
val configMapAggByDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByDay = Config(configMapAggByDay)

val configMapAggByMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByMonth = Config(configMapAggByMonth)

val configMapAggByHour = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByHour,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByHour = Config(configMapAggByHour)

val configMapAggLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggLatestTimeStamp = Config(configMapAggLatestTimeStamp)

val configMapRawLive = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionLiveRaw,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize
 // "CheckpointLocation" -> cdbCheckpoint
)
val configRawLive = Config(configMapRawLive)


// COMMAND ----------

Metric9003.coalesce(10).write.mode(SaveMode.Overwrite).cosmosDB(configRawLive)

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val DfMetric9003 = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/test9003/intelsat-9003-July-Sept")
  .filter($"unixTimestamp"< 1601510400)


  //.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
  //.select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
  //.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
  
//  .filter($"Datestamp" >= (date_sub(current_timestamp(), 30) cast StringType))

// COMMAND ----------

DfMetric9003.printSchema

// COMMAND ----------

val DfMetric9003Day =  DfMetric9003
.withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM-dd"),"yyyy-MM-dd"))
.select("unixTimestamp", "valueInDouble",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

// COMMAND ----------

println(DfMetric9003Day.count())

// COMMAND ----------

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggTypeDay = DfMetric9003Day.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")


//val InputDFAggType = InputDF.join(dfMetricAggType).where(InputDF("kymetaMetricId") === dfMetricAggType("id") and ((dfMetricAggType("aggregationType") == "SUM") or (dfMetricAggType("aggregationType") == "AVG"))).select("unixDateStamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "value")


// COMMAND ----------

println(InputDFAggTypeDay.count())

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFDay = InputDFAggTypeDay.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )


// COMMAND ----------

val aggregatesDFDayCosmos = aggregatesDFDay
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

//display(aggregatesDFDayCosmos.orderBy($"unixTimestamp"))
println(aggregatesDFDayCosmos.count())

// COMMAND ----------

import org.apache.spark.sql.SaveMode
aggregatesDFDayCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByDay)

// COMMAND ----------

val DfMetric9003Month =  DfMetric9003
.withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM"),"yyyy-MM"))
.select("unixTimestamp", "valueInDouble",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

// COMMAND ----------

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggTypeMonth = DfMetric9003Month.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")


//val InputDFAggType = InputDF.join(dfMetricAggType).where(InputDF("kymetaMetricId") === dfMetricAggType("id") and ((dfMetricAggType("aggregationType") == "SUM") or (dfMetricAggType("aggregationType") == "AVG"))).select("unixDateStamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "value")


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFMonth = InputDFAggTypeMonth.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId","categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )



// COMMAND ----------

val aggregatesDFMonthCosmos = aggregatesDFMonth
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

display(aggregatesDFMonthCosmos)

// COMMAND ----------

aggregatesDFMonthCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

val DfMetric9003Hour =  DfMetric9003
.withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM-dd HH"),"yyyy-MM-dd HH"))
.select("unixTimestamp", "valueInDouble",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

// COMMAND ----------

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggTypeHour = DfMetric9003Hour.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")


//val InputDFAggType = InputDF.join(dfMetricAggType).where(InputDF("kymetaMetricId") === dfMetricAggType("id") and ((dfMetricAggType("aggregationType") == "SUM") or (dfMetricAggType("aggregationType") == "AVG"))).select("unixDateStamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "value")


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFHour = InputDFAggTypeHour.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )



// COMMAND ----------

val aggregatesDFHourCosmos = aggregatesDFHour
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

aggregatesDFHourCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)