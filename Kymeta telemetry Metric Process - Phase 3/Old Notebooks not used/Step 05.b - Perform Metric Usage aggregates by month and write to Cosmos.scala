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
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"metricProviderId" === 1)
  //.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId"))).dropDuplicates("id")
  //.drop("id")
  .withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM"),"yyyy-MM"))
  .filter($"unixTimestamp" === 1593561600)
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

import org.apache.spark.sql._
import io.delta.tables._

val deltaTableAggMonth = DeltaTable.forName("MetricAggregatedByMonth")


  deltaTableAggMonth.as("t")
    .merge(
      aggregatesDF.as("s"), 
      "s.unixTimestamp = t.unixTimestamp and s.remoteId = t.remoteId and s.kymetaMetricId = t.kymetaMetricId and s.metricProviderId = t.metricProviderId and s.categoryId = t.categoryId")
    .whenMatched().updateAll()
    //.whenNotMatched().insertAll()
    .execute()


// COMMAND ----------

val DfMetricAggMonthCosmos = aggregatesDF
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

println(aggregatesDF.count())

// COMMAND ----------

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider
// Configure the connection to your collection in Cosmos DB.
// Please refer to https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references
// for the description of the available configurations.
val configMapAggByDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByDay = Config(configMapAggByDay)

val configMapAggByMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByMonth = Config(configMapAggByMonth)

val configMapAggByHour = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByHour,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByHour = Config(configMapAggByHour)



// COMMAND ----------

DfMetricAggMonthCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

// MAGIC %md Check that the data in the Delta table is updating by running the following multiple times.

// COMMAND ----------

//display(sql("select * from MetricGoldAggMonth order by YearMonth"))