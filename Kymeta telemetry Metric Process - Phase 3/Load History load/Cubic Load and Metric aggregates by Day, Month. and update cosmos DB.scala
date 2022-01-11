// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC This notebook shows how you can write the output of a streaming aggregation as upserts into a Delta table using the `foreachBatch` and `merge` operations.
// MAGIC This writes the aggregation output in *update mode* which is a *lot more* scalable that writing aggregations in *complete mode*.

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

// MAGIC %md 
// MAGIC #Load Cubic Metrics from Bronze table

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFCubicBronze = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Cubic-bronze")
  .select($"ICCID" as "serialnumber",round($"MBCharged".cast(DoubleType)*1024*1024).cast(LongType).cast(StringType) as "MBCharged",$"MCC",$"timestamp".cast(StringType) as "unix_timestamp",$"Datestamp")
  .filter($"MBCharged" =!= "0")

// COMMAND ----------

val NewSchemaCubic= StructType(
    StructField("data", ArrayType(StructType(
        StructField("Name", StringType, true) ::
          StructField("Value", StringType, true) ::
            Nil)),true) ::
  Nil)

// COMMAND ----------

val DfCubicBrJson = streamingInputDFCubicBronze
.withColumn("Jsondata", from_json(concat(lit("{\"data\":[{\"Name\": \"MBCharged\",\"Value\":\""), $"MBCharged", lit("\"},{\"Name\": \"MCC\",\"Value\":\""),$"MCC", lit("\"}]}")), NewSchemaCubic))


// COMMAND ----------

val DfCubicNameValueExplode = DfCubicBrJson
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"serialnumber",$"unix_timestamp",$"Datestamp",$"Name",$"Value")


// COMMAND ----------

val metricmappingsDFCubic = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 4)
//display(metricmappingsDFCubic)

// COMMAND ----------

val CubicJoinMetricMappingDF = DfCubicNameValueExplode.join(
    metricmappingsDFCubic,
    expr(""" 
       rawSymbol = Name
      """
    )
  )
.select("serialnumber","unix_timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID")
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumn("element", lit(null).cast(StringType))
.withColumn("deviceType", lit("DEV_SIM"))
.withColumn("model", lit(null).cast(StringType))

// COMMAND ----------


CubicJoinMetricMappingDF
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.write
.format("delta")
.partitionBy("Datestamp")
.option("mergeSchema", "true")
.mode("overwrite")
.save(basePath + "DeltaTable/Cubic-Batch-silver-step2")

// COMMAND ----------



// COMMAND ----------

// MAGIC %md 
// MAGIC #Run Aggregation on Gold table for Cubic and send to Cosmos DB

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

//set number of days you want to process the cubic data for cal1.add(Calendar.DATE, -2)
//set number of months you want to process the cubic data for cal2.add(Calendar.MONTH, -1)
/*
cal1.setTime(currentTime);
cal1.add(Calendar.DATE, -180)
val yesterDay = dateFormat.format(cal1.getTime)

val cal2 = Calendar.getInstance
cal2.set(Calendar.DAY_OF_MONTH,cal2.getActualMinimum(Calendar.DAY_OF_MONTH));
cal2.add(Calendar.MONTH, -6)

val FirstDayMonth = dateFormat.format(cal2.getTime)

val cal1 = Calendar.getInstance
def currentTime = new java.util.Date();
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
*/

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
  .filter($"metricProviderId" === 4)
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

display(aggregatesDFDay)

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

display(aggregatesDFMonth.filter($"remoteId".isin("19ea86b5-a760-491f-b256-6d2d7ee928e7","e10c4c66-5505-4ff9-bd4c-eb344ebc43c9","59417513-6fbc-4b29-9748-eb16142bc66a","276e9cfe-f86f-4e73-8d49-3573377b8455")))

// COMMAND ----------

// Write to Cosmos DB as stream
 
  aggregatesDFMonth
  .write
   .format("cosmos.oltp")
   .options(configMapAggByMonth)
   .mode("APPEND")
   .save()
