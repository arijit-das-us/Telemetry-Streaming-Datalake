// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

val configMapAggLatest = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseRaw,
  "spark.cosmos.container" -> cdbCollectionAggLatestTime,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
  "spark.cosmos.write.maxRetryCount" -> "50",
  "spark.cosmos.write.strategy" -> "ItemOverwrite")

// COMMAND ----------

// Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cdbEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cdbMasterkey)


// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat

val cal1 = Calendar.getInstance
def currentTime = new java.util.Date();
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


val cal3 = Calendar.getInstance();
//cal3.add(Calendar.HOUR_OF_DAY, -1);
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

val dfLatestMetricStream1 = spark.readStream
  .format("delta")
  .option("startingTimestamp", oneHoursBack)
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .filter($"metricProviderId".isin(1,2,3,4) && $"kymetaMetricId".isin(130,131,9004,108,229,906,907,116) )


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfLatestMetricStreamCosmos1 = dfLatestMetricStream1
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("unixTimestamp", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))


// COMMAND ----------

// Write to Cosmos DB as stream
import org.apache.spark.sql._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "LatestStreamPool1")

dfLatestMetricStreamCosmos1
.writeStream
.queryName("LatestStreamQuery1")
.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
  
  batchDF
  .write
   .format("cosmos.oltp")
   .options(configMapAggLatest)
   .mode("APPEND")
   .save()
}
.outputMode("update")
  .start()


// COMMAND ----------


/*
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfLatestMetricStream2 = spark.readStream
  .format("delta")
  .option("startingVersion", "1257128")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter(($"metricProviderId".isin(1,2,3,4) && $"kymetaMetricId".isin(130,131,9004,108,229,906,907,116)) === false)
  */


// COMMAND ----------

/*
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfLatestMetricStreamCosmos2 = dfLatestMetricStream2
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("unixTimestamp", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
*/


// COMMAND ----------

/*
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "LatestStreamPool2")

dfLatestMetricStreamCosmos2
.writeStream
.queryName("LatestStreamQuery2")
.format("cosmos.oltp")
.outputMode("append")
.options(configMapAggLatestHub)
.option("checkpointLocation", cdbCPLatestMetric2)
.start()
*/