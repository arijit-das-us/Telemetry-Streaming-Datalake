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
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 7)))
//  .select("unixHour", "value",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingOutputDf = 
streamingInputDF.groupBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((max($"unixTimestamp")).alias("maxUnixTimestamp"))



// COMMAND ----------

val GoldDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 7)))

// COMMAND ----------

//val GoldDF1 = 
//spark.read
//  .format("delta")
//  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))

//val OutputDf1 = 
//GoldDF1.groupBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
//.agg((max($"unixTimestamp")).alias("maxUnixTimestamp"))
//println(OutputDf1.count())

// COMMAND ----------

val streamingDfWithValue =  streamingOutputDf
.join(GoldDF).where(GoldDF("remoteId") === streamingOutputDf("remoteId") && GoldDF("kymetaMetricId") === streamingOutputDf("kymetaMetricId") && GoldDF("metricProviderId") === streamingOutputDf("metricProviderId") && GoldDF("unixTimestamp") === streamingOutputDf("maxUnixTimestamp") && GoldDF("categoryId") === streamingOutputDf("categoryId"))
.select(streamingOutputDf("remoteId"),streamingOutputDf("kymetaMetricId"),streamingOutputDf("metricProviderId"),streamingOutputDf("categoryId"),GoldDF("unixTimestamp"),GoldDF("valueInDouble"),GoldDF("valueInString"))
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

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

import org.apache.spark.sql._
import io.delta.tables._


def upsertToComsmos(microBatchOutputDF: DataFrame, batchId: Long) {
  // ===================================================
  // For DBR 6.0 and above, you can use Merge Scala APIs
  // ===================================================
val DfMetricLatestRawCosmos = microBatchOutputDF.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString").withColumn("valueInDouble", $"valueInDouble".cast(DoubleType)).coalesce(10)
DfMetricLatestRawCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatestTimeStamp)
  
}




// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
streamingDfWithValue.writeStream
  .format("delta")
  .foreachBatch(upsertToComsmos _)
  .outputMode("update")
  .start()


// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

//display(sql("select * from MetricAggLatest order by remoteId"))