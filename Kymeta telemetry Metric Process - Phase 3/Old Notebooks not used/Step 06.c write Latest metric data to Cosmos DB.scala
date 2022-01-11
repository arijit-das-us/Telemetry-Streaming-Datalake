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
  //.filter($"currentTimestamp" >= unix_timestamp() - lit(900))
  .filter($"dateStamp" === current_date() && $"currentTimestamp" >= unix_timestamp() - lit(900))
  .filter($"metricProviderId" === 3) 
//display(dfLiveMetricStream)

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
dfLatestMetric.printSchema()

// COMMAND ----------

//display(dfLatestMetric)

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

// COMMAND ----------

//display(dfTopByWindows.orderBy($"unixTimestamp".desc))

// COMMAND ----------

/*
case class Record(id: String, unixTimestamp: Long, valueInDouble: Option[Double], valueInString: String, kymetaMetricId: Long, metricProviderId: Long, categoryId: Long, remoteId: String)
                  
val dfTopByCaseClass = dfLatestMetric
.as[Record]
.groupByKey(_.id)
.reduceGroups((x, y) => if (x.unixTimestamp > y.unixTimestamp) x else y)
*/

// COMMAND ----------

/*
val dfTop = dfLatestMetric.select($"id", struct($"unixTimestamp", $"valueInDouble", $"valueInString", $"kymetaMetricId", $"metricProviderId", $"categoryId", $"remoteId").alias("vs"))
  .groupBy($"id")
  .agg(max("vs").alias("vs"))
  .select($"id", $"vs.unixTimestamp", $"vs.valueInDouble", $"vs.valueInString", $"vs.kymetaMetricId", $"vs.metricProviderId", $"vs.categoryId", $"vs.remoteId")
  */

// COMMAND ----------

/*
val dfMax = dfLatestMetric.groupBy($"id".as("max_id")).agg(max($"unixTimestamp").as("max_unixTimestamp"))

val dfTopByJoin = dfLatestMetric.join(broadcast(dfMax),
    ($"id" === $"max_id") && ($"unixTimestamp" === $"max_unixTimestamp"))
  .drop("max_id")
  .drop("max_unixTimestamp")
  */