// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")

// COMMAND ----------

// Reset the output aggregates table
//val metric_gold_Agg_Day = basePath + "DeltaTable/Metric-gold-Agg-Day"

//Seq.empty[(Long, String,Long,Long,Long,BigDecimal,BigDecimal,BigDecimal,BigDecimal,Long )].toDF("unixTimestamp", "remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue", "currentTimestamp")
//.write
//.format("delta")
//.mode("append")
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

import org.apache.spark.sql._
import io.delta.tables._
val deltaTableAggDay = DeltaTable.forName("MetricAggregatedByDay")

// Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDeltaDayAgg(microBatchOutputDF: DataFrame, batchId: Long) {
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
val InputDFDayDelimeter = 
spark.readStream
  .format("delta")
  //.option("startingTimestamp", date_sub(current_date(), 1))
  .option("startingTimestamp", "2020-10-01")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))
  .filter($"valueInDouble" =!= 0)


// COMMAND ----------

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")
/*
val InputDFAggTypeDay = InputDFDayDelimeter.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")
*/

val InputDFAggTypeDay = InputDFDayDelimeter
.join(dfMetricAggType)
.where(InputDFDayDelimeter("kymetaMetricId") === dfMetricAggType("id") and (dfMetricAggType("aggregationType") === "SUM" || dfMetricAggType("aggregationType") === "AVG"))
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFDay = InputDFAggTypeDay.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldAggDayPool1")


aggregatesDFDay.writeStream
  .queryName("GoldAggDayQuery1")
  .format("delta")
  .foreachBatch(upsertToDeltaDayAgg _)
  .outputMode("update")
  .start()


// COMMAND ----------

//import spark.sqlContext.implicits._

//Get current Date & Time
//val df = Seq((1)).toDF("seq")

//val curDate = df.withColumn("current_date",current_date().as("current_date"))
//    .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
//curDate.show(false)

// COMMAND ----------

import org.apache.spark.sql._
import io.delta.tables._

val deltaTableAggMonth = DeltaTable.forName("MetricAggregatedByMonth")

// Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDeltaMonthAgg(microBatchOutputDF: DataFrame, batchId: Long) {
  // ===================================================
  // For DBR 6.0 and above, you can use Merge Scala APIs
  // ===================================================
  deltaTableAggMonth.as("t")
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
val InputDFMonthDelimeter = 
spark.readStream
  .format("delta")
  //.option("startingTimestamp", date_sub(current_date(), 1))
  .option("startingTimestamp", "2020-10-01")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("unixTimestamp", unix_timestamp(substring($"dateStamp",0,7),"yyyy-MM"))
  .filter($"valueInDouble" =!= 0)


// COMMAND ----------

val InputDFAggTypeMonth = InputDFMonthDelimeter
.join(dfMetricAggType)
.where(InputDFMonthDelimeter("kymetaMetricId") === dfMetricAggType("id") and (dfMetricAggType("aggregationType") === "SUM" || dfMetricAggType("aggregationType") === "AVG"))
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFMonth = InputDFAggTypeMonth.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId","categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldAggMonthPool2")

aggregatesDFMonth.writeStream
  .queryName("GoldAggMonthQuery2")
  .format("delta")
  .foreachBatch(upsertToDeltaMonthAgg _)
  .outputMode("update")
  .start()


// COMMAND ----------

import org.apache.spark.sql._
import io.delta.tables._

val deltaTableAggHour = DeltaTable.forName("MetricAggregatedByHour")

// Function to upsert `microBatchOutputDF` into Delta table using MERGE
def upsertToDeltaHourAgg(microBatchOutputDF: DataFrame, batchId: Long) {
  // ===================================================
  // For DBR 6.0 and above, you can use Merge Scala APIs
  // ===================================================
  deltaTableAggHour.as("t")
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
val InputDFHourDelimeter = 
spark.readStream
  .format("delta")
  //.option("startingTimestamp", date_sub(current_date(), 1))
  .option("startingTimestamp", "2020-10-01")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM-dd HH"),"yyyy-MM-dd HH"))
  .filter($"valueInDouble" =!= 0)


// COMMAND ----------

val InputDFAggTypeHour = InputDFHourDelimeter
.join(dfMetricAggType)
.where(InputDFHourDelimeter("kymetaMetricId") === dfMetricAggType("id") and (dfMetricAggType("aggregationType") === "SUM" || dfMetricAggType("aggregationType") === "AVG"))
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFHour = InputDFAggTypeHour.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldAggHourPool3")

aggregatesDFHour.writeStream
  .queryName("GoldAggHourQuery3")
  .format("delta")
  .foreachBatch(upsertToDeltaHourAgg _)
  .outputMode("update")
  .start()
