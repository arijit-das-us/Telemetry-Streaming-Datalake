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
val InputDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/History-gold-raw")
  .filter($"yearmonth" === "2020-07" || $"yearmonth" === "2020-08")
  //.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId"))).dropDuplicates("id")
  //.drop("id")
  .withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM"),"yyyy-MM"))
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30))), 
  .select("unixTimestamp", "valueInDouble",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

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
val aggregatesDF = InputDFAggType.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId","categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )



// COMMAND ----------

display(aggregatesDF.filter($"kymetaMetricId" === 44 && $"metricProviderId" === 1 && $"unixTimestamp" === 1593561600 && $"remoteId" === "ef1c30e1-7e2f-484c-8321-8378bca3c49e"))

// COMMAND ----------

println(aggregatesDF.count())

// COMMAND ----------

// Start the query to continuously upsert into aggregates tables in update mode
aggregatesDF
.withColumn("yearmonth",from_unixtime($"unixTimestamp","yyyy-MM"))
  .write
  .format("delta")
  .partitionBy("yearmonth")
  .mode("overwrite")
  .save(basePath + "DeltaTable/History-Agg-Month")

// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

//display(sql("select * from MetricGoldAggMonth order by YearMonth"))