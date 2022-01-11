// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val dfLiveMetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"remoteId" === "63ed0df9-24be-4228-b52f-e8bfab59cadb")
  .filter($"dateStamp" >= "2021-10-01")// && $"currentTimestamp" >= unix_timestamp() - lit(600))
//  .filter($"metricProviderId" === 3)
 // .filter($"metricProviderId" === 3 && $"kymetaMetricId".isin(40,75,108,116,120,229,9003))
 // .filter($"metricProviderId" === 3) 
//display(dfLiveMetricStream)

// COMMAND ----------

display(dfLiveMetric.orderBy("unixTimestamp"))

// COMMAND ----------

val configMapAggLatest = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggLatest = Config(configMapAggLatest)

// COMMAND ----------

val dfLatestMetric = dfLiveMetric
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("unixTimestamp", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
//dfLatestMetric.printSchema()

// COMMAND ----------

//https://stackoverflow.com/questions/33878370/how-to-select-the-first-row-of-each-group/33878701

import org.apache.spark.sql.functions.{row_number, max, broadcast}
import org.apache.spark.sql.expressions.Window

val w3 = Window.partitionBy("id").orderBy(col("unixTimestamp").desc)
    
val dfTopByWindows = dfLatestMetric
.withColumn("row",row_number.over(w3))
.where($"row" === 1).drop("row")
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

dfTopByWindows.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatest)