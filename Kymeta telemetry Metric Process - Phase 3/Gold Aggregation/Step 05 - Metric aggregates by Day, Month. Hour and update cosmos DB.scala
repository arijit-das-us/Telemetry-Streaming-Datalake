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

val cal2 = Calendar.getInstance
cal2.set(Calendar.DAY_OF_MONTH,cal2.getActualMinimum(Calendar.DAY_OF_MONTH));
cal2.add(Calendar.DATE, -1)

val lastDayOflastMonth = dateFormat.format(cal2.getTime)


val cal3 = Calendar.getInstance();
cal3.add(Calendar.HOUR_OF_DAY, -6);
val sixHoursBack = timeFormat.format(cal3.getTime)


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFDayDelimeter = 
spark.readStream
  .format("delta")
  .option("startingTimestamp", yesterDay)
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-raw")
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

// Write to Cosmos DB as stream
import org.apache.spark.sql._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "AggDayStreamPool1")

aggregatesDFDay
.writeStream
.queryName("AggDayStreamQuery1")
.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
  batchDF
  .write
   .format("cosmos.oltp")
   .options(configMapAggByDay)
   .mode("APPEND")
   .save()
}
.outputMode("update")
  .start()


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFMonthDelimeter = 
spark.readStream
  .format("delta")
  .option("startingTimestamp", lastDayOflastMonth)
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("unixTimestamp", unix_timestamp(substring($"dateStamp",0,7),"yyyy-MM"))
  .filter($"unixTimestamp" === unix_timestamp(trunc(current_date(),"Month"),"yyyy-MM-dd") )
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
import org.apache.spark.sql._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "AggMonthStreamPool1")

aggregatesDFMonth
.writeStream
.queryName("AggMonthStreamQuery1")
.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
  
  batchDF
  .write
   .format("cosmos.oltp")
   .options(configMapAggByMonth)
   .mode("APPEND")
   .save()
}
.outputMode("update")
  .start()


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFHourDelimeter = 
spark.readStream
  .format("delta")
  //.option("startingTimestamp", date_sub(current_date(), 1))
  .option("startingTimestamp", sixHoursBack)
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM-dd HH"),"yyyy-MM-dd HH"))
  .filter($"valueInDouble" =!= 0 && ($"aggregationType" === "SUM" || $"aggregationType" === "AVG"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFHour = InputDFHourDelimeter.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))



// COMMAND ----------

// Write to Cosmos DB as stream
import org.apache.spark.sql._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "AggHourStreamPool1")

aggregatesDFHour
.writeStream
.queryName("AggHourStreamQuery1")
.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
  
  batchDF
  .write
   .format("cosmos.oltp")
   .options(configMapAggByHour)
   .mode("APPEND")
   .save()
}
.outputMode("update")
  .start()
