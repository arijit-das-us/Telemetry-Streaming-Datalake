// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val dfLatestMetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-latest")
  .filter($"currentTimestamp" > (unix_timestamp() - lit(900)))
 // .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

val GoldDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  //.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"unixTimestamp" > (unix_timestamp() - lit(172800)))
.filter($"dateStamp" === "2020-08-24")

// COMMAND ----------

//val GoldDfStreamWithWatermark = GoldDFStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")
//val LatestMetricStreamWatermark = dfLatestMetricStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")

// COMMAND ----------

val LatestDfWithValue =  dfLatestMetric
.join(GoldDF).where(GoldDF("remoteId") === dfLatestMetric("remoteId") && GoldDF("kymetaMetricId") === dfLatestMetric("kymetaMetricId") && GoldDF("metricProviderId") === dfLatestMetric("metricProviderId") && GoldDF("unixTimestamp") === dfLatestMetric("unixTimestamp") && GoldDF("categoryId") === dfLatestMetric("categoryId"))
.select(dfLatestMetric("remoteId"),dfLatestMetric("kymetaMetricId"),dfLatestMetric("metricProviderId"),dfLatestMetric("categoryId"),dfLatestMetric("unixTimestamp"),GoldDF("valueInDouble"),GoldDF("valueInString"))
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider

val configMapLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize
  //"CheckpointLocation" -> cdbCheckpointLatestMetricTest
  )
val configLatestTimeStamp = Config(configMapLatestTimeStamp)

// COMMAND ----------

val dfLatestMetricCosmos = LatestDfWithValue
.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString")
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType)).coalesce(32)

// COMMAND ----------

dfLatestMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configLatestTimeStamp)