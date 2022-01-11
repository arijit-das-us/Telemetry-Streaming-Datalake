// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

// Reset the output aggregates table
val metric_gold_LatestRaw = basePath + "DeltaTable/Metric-gold-Agg-latest"

Seq.empty[(String,Long,Long,Long,Long,BigDecimal,String)].toDF("remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString")
.write
.format("delta")
.mode("overwrite")
.partitionBy("remoteId")
.option("path",metric_gold_LatestRaw)
.save

// COMMAND ----------

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggLatest 
  USING DELTA 
  OPTIONS (path = "$metric_gold_LatestRaw")
  """)

// COMMAND ----------

// DBTITLE 1,Upsert into Delta Table
import org.apache.spark.sql._
import io.delta.tables._

val deltaTableLatestRaw = DeltaTable.forName("MetricAggLatest")

// Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
  // ===================================================
  // For DBR 6.0 and above, you can use Merge Scala APIs
  // ===================================================
  deltaTableLatestRaw.as("t")
    .merge(
      microBatchOutputDF.as("s"), 
      "s.remoteId = t.remoteId and s.kymetaMetricId = t.kymetaMetricId and s.metricProviderId = t.metricProviderId and s.categoryId = t.categoryId")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}




// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val streamingInputDF = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
  .filter($"metricProviderId" =!= 3)
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
  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))

// COMMAND ----------

val streamingDfWithValue =  streamingOutputDf
.join(GoldDF).where(GoldDF("remoteId") === streamingOutputDf("remoteId") && GoldDF("kymetaMetricId") === streamingOutputDf("kymetaMetricId") && GoldDF("metricProviderId") === streamingOutputDf("metricProviderId") && GoldDF("unixTimestamp") === streamingOutputDf("maxUnixTimestamp") && GoldDF("categoryId") === streamingOutputDf("categoryId"))
.select(streamingOutputDf("remoteId"),streamingOutputDf("kymetaMetricId"),streamingOutputDf("metricProviderId"),streamingOutputDf("categoryId"),GoldDF("unixTimestamp"),GoldDF("valueInDouble"),GoldDF("valueInString"))

// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
streamingDfWithValue.writeStream
  .format("delta")
  .foreachBatch(upsertToDelta _)
  .outputMode("update")
  .start()


// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

//display(sql("select * from MetricAggLatest order by remoteId"))