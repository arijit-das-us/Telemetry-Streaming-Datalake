// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val streamingInputDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"unixTimestamp" > (unix_timestamp() - lit(3600)))
 // .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 7)))
//  .select("unixHour", "value",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingOutputDf = 
streamingInputDF.groupBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((max($"unixTimestamp")).alias("maxUnixTimestamp"))


// COMMAND ----------

//println(streamingOutputDf.count())

// COMMAND ----------

val GoldDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"unixTimestamp" > (unix_timestamp() - lit(3600)))
 // .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
 // .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 7)))

// COMMAND ----------

val streamingDfWithValue =  streamingOutputDf
.join(GoldDF).where(GoldDF("remoteId") === streamingOutputDf("remoteId") && GoldDF("kymetaMetricId") === streamingOutputDf("kymetaMetricId") && GoldDF("metricProviderId") === streamingOutputDf("metricProviderId") && GoldDF("unixTimestamp") === streamingOutputDf("maxUnixTimestamp") && GoldDF("categoryId") === streamingOutputDf("categoryId"))
.select(streamingOutputDf("remoteId"),streamingOutputDf("kymetaMetricId"),streamingOutputDf("metricProviderId"),streamingOutputDf("categoryId"),streamingOutputDf("maxUnixTimestamp") as "unixTimestamp",GoldDF("valueInDouble"),GoldDF("valueInString"))
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

//println(streamingDfWithValue.count())

// COMMAND ----------

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

val configMapAggLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggLatestTimeStamp = Config(configMapAggLatestTimeStamp)

// COMMAND ----------

val DfMetricLatestRawCosmos = streamingDfWithValue.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString").withColumn("valueInDouble", $"valueInDouble".cast(DoubleType)).coalesce(32)
DfMetricLatestRawCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatestTimeStamp)