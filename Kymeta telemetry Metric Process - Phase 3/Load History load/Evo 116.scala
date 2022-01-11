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


val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")


// COMMAND ----------

val factCustommetrics = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/custommetrics")

// COMMAND ----------

display(factCustommetrics)

// COMMAND ----------

val streamingInputDFGold = 
spark.read
  .format("delta")
  //.option("startingTimestamp", "2020-10-01")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"metricProviderId" === 3)

// COMMAND ----------

val dfcustomMetricSUM = streamingInputDFGold.join(factCustommetrics).where(streamingInputDFGold("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "SUM")) 
.select("unixTimestamp","dateStamp", "valueInDouble", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType","currentTimestamp")



// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfCustomMetricsAggSumUp = dfcustomMetricSUM
.groupBy($"unixTimestamp",$"RemoteId", $"metricProviderId", $"categoryId", $"metricId")
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

// COMMAND ----------

//display(dfCustomMetricsSumNormalized)

// COMMAND ----------


val dfExplodedQuery2 =  
dfCustomMetricsSumNormalized
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.write
.format("delta")
.partitionBy("Datestamp")
.mode("overwrite")
.save(basePath + "DeltaTable/test9003/Metric-gold-evo-9003-sum")                                                  // Append new records to the output path

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFGoldRawEvo9003 = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/test9003/Metric-gold-evo-9003-sum")
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble","dateStamp")
  .dropDuplicates()

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFMonth = InputDFGoldRawEvo9003
.withColumn("unixTimestamp", unix_timestamp(substring($"dateStamp",0,7),"yyyy-MM"))
.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )

// COMMAND ----------

//display(aggregatesDFMonth.filter($"unixTimestamp" === "1601510400"))

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
.filter($"unixTimestamp" =!= 1604188800)
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

println(MetricAggMonthCosmos.count())

// COMMAND ----------

import org.apache.spark.sql.SaveMode
MetricAggMonthCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFDay = InputDFGoldRawEvo9003
.withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))
.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFHour = InputDFGoldRawEvo9003
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

display(MetricAggDayCosmos.filter($"unixTimestamp" === 1602115200 and $"remoteId" === "5f930912-99ba-4e7b-b462-818ed865bfc3"))

// COMMAND ----------

display(MetricAggDayCosmos.filter($"unixTimestamp" === 1602201600 and $"remoteId" === "5f930912-99ba-4e7b-b462-818ed865bfc3"))

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

println(MetricAggDayCosmos.count())

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

println(MetricAggHourCosmos.count())

// COMMAND ----------

MetricAggHourCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val DfMetricRawCosmos = spark.read
.format("delta")
.load(basePath + "DeltaTable/testHistory/EVO-Metric-gold-116")
//.filter($"dateStamp".startsWith("2020-11"))
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.coalesce(42)

// COMMAND ----------

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

println(DfMetricRawCosmos.count())

// COMMAND ----------

DfMetricRawCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configRawLive)

// COMMAND ----------

display(DfMetricRawCosmos)

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

val dfLatestMetric = spark.read
.format("delta")
.load(basePath + "DeltaTable/testHistory/EVO-Metric-gold-116")
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

display(dfTopByWindows)

// COMMAND ----------

dfTopByWindows.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatest)