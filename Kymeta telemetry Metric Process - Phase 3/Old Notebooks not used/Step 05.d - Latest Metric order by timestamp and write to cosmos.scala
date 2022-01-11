// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val streamingInputDF = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"unixTimestamp" > (unix_timestamp() - lit(172800)))
 // .filter($"unixTimestamp" > lit(1597017600))
 // .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
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
  .filter($"unixTimestamp" > (unix_timestamp() - lit(172800)))
//  .filter($"unixTimestamp" > lit(1597017600))
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
// .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))

// COMMAND ----------

//val GoldDF1 = 
//spark.read
//  .format("delta")
//  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))

//val OutputDf1 = 
//GoldDF.groupBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
//.agg((max($"unixTimestamp")).alias("maxUnixTimestamp"))
//println(OutputDf1.count())

// COMMAND ----------

val streamingDfWithValue =  streamingOutputDf
.join(GoldDF).where(GoldDF("remoteId") === streamingOutputDf("remoteId") && GoldDF("kymetaMetricId") === streamingOutputDf("kymetaMetricId") && GoldDF("metricProviderId") === streamingOutputDf("metricProviderId") && GoldDF("unixTimestamp") === streamingOutputDf("maxUnixTimestamp") && GoldDF("categoryId") === streamingOutputDf("categoryId"))
.select(streamingOutputDf("remoteId"),streamingOutputDf("kymetaMetricId"),streamingOutputDf("metricProviderId"),streamingOutputDf("categoryId"),GoldDF("unixTimestamp"),GoldDF("valueInDouble"),GoldDF("valueInString"))
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString")
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))

// COMMAND ----------

//display(streamingDfWithValue)

// COMMAND ----------

val configMapAggLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize,
  "CheckpointLocation" -> cdbCheckpointLatestMetric)
val configAggLatestTimeStamp = Config(configMapAggLatestTimeStamp)

// COMMAND ----------

// Write to Cosmos DB as stream
streamingDfWithValue.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("update").options(configMapAggLatestTimeStamp).option("checkpointLocation", cdbCheckpointLatestMetric).start()



// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

//display(sql("select * from MetricAggLatest order by remoteId"))