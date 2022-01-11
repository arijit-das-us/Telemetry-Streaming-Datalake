// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val latestMetric = table("MetricLatest")
.select("remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","currentTimestamp")
.filter($"metricProviderId" =!= 2)
.filter($"currentTimestamp" > (unix_timestamp() - lit(1800)))
//.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//.filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 1)))


// COMMAND ----------

//val countlatestmetric = customRdd.count()

// COMMAND ----------

//println(streamingOutputDf.count())

// COMMAND ----------

val GoldDF = table("MetricGold")
  .filter($"metricProviderId" =!= 2)
  .filter($"unixTimestamp" > (unix_timestamp() - lit(3600)))

// COMMAND ----------

val LatestDfWithValue =  latestMetric
.join(GoldDF).where(GoldDF("remoteId") === latestMetric("remoteId") && GoldDF("kymetaMetricId") === latestMetric("kymetaMetricId") && GoldDF("metricProviderId") === latestMetric("metricProviderId") && GoldDF("unixTimestamp") === latestMetric("unixTimestamp") && GoldDF("categoryId") === latestMetric("categoryId"))
.select(latestMetric("remoteId"),latestMetric("kymetaMetricId"),latestMetric("metricProviderId"),latestMetric("categoryId"),latestMetric("unixTimestamp") ,GoldDF("valueInDouble"),GoldDF("valueInString"))
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

//println(LatestDfWithValue.count())

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

val DfMetricLatestRawCosmos = LatestDfWithValue.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString").withColumn("valueInDouble", $"valueInDouble".cast(DoubleType)).coalesce(32)
DfMetricLatestRawCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatestTimeStamp)