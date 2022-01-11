// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

val configMapAggByDay = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseAgg,
  "spark.cosmos.container" -> cdbCollectionAggByDay,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
  "spark.cosmos.write.maxRetryCount" -> "50",
  "spark.cosmos.write.point.maxConcurrency" -> "100",
  "spark.cosmos.useGatewayMode" -> "true",
  "spark.cosmos.write.strategy" -> "ItemOverwrite")

val configMapAggByMonth = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseAgg,
  "spark.cosmos.container" -> cdbCollectionAggByMonth,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
  "spark.cosmos.write.maxRetryCount" -> "50",
  "spark.cosmos.write.point.maxConcurrency" -> "100",
  "spark.cosmos.useGatewayMode" -> "true",
  "spark.cosmos.write.strategy" -> "ItemOverwrite")

val configMapAggByHour = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseAgg,
  "spark.cosmos.container" -> cdbCollectionAggByHour,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
  "spark.cosmos.write.maxRetryCount" -> "50",
  "spark.cosmos.write.point.maxConcurrency" -> "100",
  "spark.cosmos.useGatewayMode" -> "true",
  "spark.cosmos.write.strategy" -> "ItemOverwrite")


// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat

val cal1 = Calendar.getInstance
def currentTime = new java.util.Date();
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

cal1.setTime(currentTime);
cal1.add(Calendar.DATE, -1)
val yesterDay = dateFormat.format(cal1.getTime)


val FirstDayMonth = yesterDay.substring(0,8) + "01"



// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFGold = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
 // .filter($"remoteId" === "0cf66209-8625-4015-92eb-ce6cef5070c6")
  .filter($"kymetaMetricId" === 9003)
  .filter($"dateStamp" >= FirstDayMonth)
  .dropDuplicates("unixTimestamp", "remoteId", "metricProviderId", "kymetaMetricId", "categoryId")
  

// COMMAND ----------

val InputDFDayDelimeter = InputDFGold
//.filter($"dateStamp" === yesterDay)
.withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))
  .filter($"valueInDouble" =!= 0 && ($"aggregationType" === "SUM" || $"aggregationType" === "AVG"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFDay = InputDFDayDelimeter.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))

// COMMAND ----------

 aggregatesDFDay
  .write
   .format("cosmos.oltp")
   .options(configMapAggByDay)
   .mode("APPEND")
   .save()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFMonthDelimeter = InputDFGold
  .withColumn("unixTimestamp", unix_timestamp(substring($"dateStamp",0,7),"yyyy-MM"))
//  .filter($"unixTimestamp" === unix_timestamp(trunc(current_date(),"Month"),"yyyy-MM-dd") )
  .filter($"valueInDouble" =!= 0 && ($"aggregationType" === "SUM" || $"aggregationType" === "AVG"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFMonth = InputDFMonthDelimeter.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId","categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))


// COMMAND ----------

// Write to Cosmos DB as stream
 
  aggregatesDFMonth
  .write
   .format("cosmos.oltp")
   .options(configMapAggByMonth)
   .mode("APPEND")
   .save()
