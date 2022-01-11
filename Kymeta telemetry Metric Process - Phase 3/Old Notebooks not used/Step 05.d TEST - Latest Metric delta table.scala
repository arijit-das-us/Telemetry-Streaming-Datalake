// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

// Reset the output aggregates table
val metric_gold_LatestRaw = basePath + "DeltaTable/Metric-gold-latest"

Seq.empty[(String,Long,Long,Long,Long,BigDecimal,String)].toDF("remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString")
.write
.format("delta")
.mode("overwrite")
.partitionBy("remoteId")
.option("path",metric_gold_LatestRaw)
.save

// COMMAND ----------

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricLatest 
  USING DELTA 
  OPTIONS (path = "$metric_gold_LatestRaw")
  """)

// COMMAND ----------

// DBTITLE 1,Upsert into Delta Table
import org.apache.spark.sql._
import io.delta.tables._

val deltaTableLatestRaw = DeltaTable.forName("MetricLatest")

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
  .load(basePath + "DeltaTable/Metric-gold-raw/")
//  .filter($"unixTimestamp" > (unix_timestamp() - lit(1209600)))
//  .withColumn("uniqueHashKey",sha1(concat(lit("latest"),$"remoteId",$"kymetaMetricId",$"metricProviderId",$"categoryId"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//  .withWatermark("TimestampYYMMSSHHMMSS", "30 seconds")
//  .dropDuplicates("uniqueHashKey","TimestampYYMMSSHHMMSS")
 // .filter($"metricProviderId" =!= 3)
  //.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  //.filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//  .select("unixHour", "value",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingOutputDf = 
streamingInputDF.groupBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((max($"unixTimestamp")).alias("maxUnixTimestamp"))



// COMMAND ----------

//import org.apache.spark.sql.expressions.Window
//val w = Window.partitionBy($"remoteId", $"kymetaMetricId", $"metricProviderId", $"categoryId")



//val streamingWindowDf = streamingInputDF
//.withColumn("maxUnixTimestamp", max($"unixTimestamp").over(w))
//.withColumn("maxUnixTimestamp", max($"unixTimestamp").groupBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId"))
//.where(col("unixTimestamp") === col("maxUnixTimestamp"))
//.drop("maxUnixTimestamp")

//df.withColumn('maxB', f.max('B').over(w))\
//    .where(f.col('B') == f.col('maxB'))\
//    .drop('maxB')\
//    .show()

// COMMAND ----------

//display(streamingWindowDf)

// COMMAND ----------

//from pyspark.sql import Window
//import pyspark.sql.functions as f
//w = Window.partitionBy('A')
//df.withColumn('maxB', f.max('B').over(w))\
//    .where(f.col('B') == f.col('maxB'))\
//    .drop('maxB')\
//    .show()

// COMMAND ----------

 val dfLatestMetricStream = streamingOutputDf
.withColumnRenamed("maxUnixTimestamp","unixTimestamp")
.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
.withWatermark("TimestampYYMMddHHmmss", "36 hours")

// COMMAND ----------

val GoldDFStream = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  //.filter($"unixTimestamp" > (unix_timestamp() - lit(172800)))
val GoldDfStreamWithWatermark = GoldDFStream.withWatermark("TimestampYYMMddHHmmss", "36 hours")

// COMMAND ----------

val streamingDfWithValue =  dfLatestMetricStream
.join(GoldDfStreamWithWatermark).where(GoldDfStreamWithWatermark("remoteId") === dfLatestMetricStream("remoteId") && GoldDfStreamWithWatermark("kymetaMetricId") === dfLatestMetricStream("kymetaMetricId") && GoldDfStreamWithWatermark("metricProviderId") === dfLatestMetricStream("metricProviderId") && GoldDfStreamWithWatermark("unixTimestamp") === dfLatestMetricStream("unixTimestamp") && GoldDfStreamWithWatermark("categoryId") === dfLatestMetricStream("categoryId"))
.select(dfLatestMetricStream("remoteId"),dfLatestMetricStream("kymetaMetricId"),dfLatestMetricStream("metricProviderId"),dfLatestMetricStream("categoryId"),dfLatestMetricStream("unixTimestamp"),GoldDfStreamWithWatermark("valueInDouble"),GoldDfStreamWithWatermark("valueInString"))
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
  "WritingBatchSize" -> cdbBatchSize,
  "CheckpointLocation" -> cdbCheckpointLatestMetricTest)
val configLatestTimeStamp = Config(configMapLatestTimeStamp)

// COMMAND ----------

// Write to Cosmos DB as stream

streamingDfWithValue.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("append").options(configMapLatestTimeStamp).option("checkpointLocation", cdbCheckpointLatestMetricTest).start()



// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
streamingDfWithValue
  .writeStream
  .option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-gold-latest-store")
  .format("delta")
  .foreachBatch(upsertToDelta _)
 // .outputMode("complete")
  .start()


// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

//display(sql("select * from MetricAggLatest order by remoteId"))