// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

val configMapRawLive = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseRaw,
  "spark.cosmos.container" -> cdbCollectionLiveRaw,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
  "spark.cosmos.write.maxRetryCount" -> "50",
  "spark.cosmos.write.point.maxConcurrency" -> "100",
  "spark.cosmos.useGatewayMode" -> "true",
  "spark.cosmos.write.strategy" -> "ItemOverwrite")


// COMMAND ----------

// Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cdbEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cdbMasterkey)


// COMMAND ----------


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val dfLiveMetricStream = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
.filter($"unixTimestamp" > lit(1634317495))
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

val dfLiveMetricStreamCosmos = dfLiveMetricStream
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

dfLiveMetricStreamCosmos
  .write
   .format("cosmos.oltp")
   .options(configMapRawLive)
   .mode("APPEND")
   .save()

// COMMAND ----------

val configMapAggLatest = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseRaw,
  "spark.cosmos.container" -> cdbCollectionAggLatestTime,
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


val cal3 = Calendar.getInstance();
cal3.add(Calendar.MINUTE, -15);
val oneHoursBack = timeFormat.format(cal3.getTime)


// COMMAND ----------

/*
Latitude=131
Longitude=130
Location=9004
SNR=108
CNO=229
Rx Throughput=906
Tx Throughput=907
Online status=116
*/

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfLatestMetricStream = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"unixTimestamp" > lit(1634317495))
//  .filter($"metricProviderId".isin(1,2,3,4) && $"kymetaMetricId".isin(130,131,9004,108,229,906,907,116) )


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfLatestMetricStreamCosmos = dfLatestMetricStream
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("unixTimestamp", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))




// COMMAND ----------

//https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group/33878701

import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

val w3 = Window.partitionBy("id").orderBy(col("unixTimestamp").desc)
    
val dfTopByWindows = dfLatestMetricStreamCosmos
.withColumn("row",row_number.over(w3))
.where($"row" === 1).drop("row")
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.coalesce(32)

// COMMAND ----------


  dfTopByWindows
  .write
   .format("cosmos.oltp")
   .options(configMapAggLatest)
   .mode("APPEND")
   .save()
