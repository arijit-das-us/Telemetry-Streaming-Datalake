// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")

// COMMAND ----------

// Reset the output aggregates table
//val metric_gold_Agg_Day = basePath + "DeltaTable/Metric-gold-Agg-Day"

//Seq.empty[(Long, String,Long,Long,Long,BigDecimal,BigDecimal,BigDecimal,BigDecimal )].toDF("unixTimestamp", "remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
//.write
//.format("delta")
//.mode("overwrite")
//.partitionBy("remoteId")
//.option("path",metric_gold_Agg_Day)
//.save

// COMMAND ----------

//spark.sql(s"""
//CREATE TABLE IF NOT EXISTS MetricAggregatedByDay 
//  USING DELTA 
//  OPTIONS (path = "$metric_gold_Agg_Day")
//  """)

// COMMAND ----------

// DBTITLE 1,Upsert into Delta Table
import org.apache.spark.sql._
import io.delta.tables._

val deltaTableAggDay = DeltaTable.forName("MetricAggregatedByDay")

// Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDelta(microBatchOutputDF: DataFrame, batchId: Long) {
  // ===================================================
  // For DBR 6.0 and above, you can use Merge Scala APIs
  // ===================================================
  deltaTableAggDay.as("t")
    .merge(
      microBatchOutputDF.as("s"), 
      "s.unixTimestamp = t.unixTimestamp and s.remoteId = t.remoteId and s.kymetaMetricId = t.kymetaMetricId and s.metricProviderId = t.metricProviderId and s.categoryId = t.categoryId")
    .whenMatched().updateAll()
    .whenNotMatched().insertAll()
    .execute()
}




// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDF = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"unixTimestamp" >= unix_timestamp(from_unixtime(unix_timestamp() - lit(1209600),"yyyy-MM-dd"),"yyyy-MM-dd"))
  .withColumn("uniqueHashKey",sha1(concat(lit("daily"),$"remoteId",$"kymetaMetricId",$"metricProviderId",$"categoryId"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  .withWatermark("TimestampYYMMSSHHMMSS", "30 seconds")
  .dropDuplicates("uniqueHashKey","TimestampYYMMSSHHMMSS")
  .drop("unixTimestamp")
  .withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))


// COMMAND ----------

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggType = InputDF.join(
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
val aggregatesDF = InputDFAggType.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )




// COMMAND ----------

//display(aggregatesDF.orderBy("Datestamp"))

// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
aggregatesDF.writeStream
  .format("delta")
  .foreachBatch(upsertToDelta _)
  .outputMode("update")
  .start()


// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

//display(sql("select * from MetricGoldAggDay order by Datestamp"))