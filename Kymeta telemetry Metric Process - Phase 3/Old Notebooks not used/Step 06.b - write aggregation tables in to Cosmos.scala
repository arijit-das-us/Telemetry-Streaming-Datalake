// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

//import org.joda.time._
//import org.joda.time.format._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val MetricAggDay = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Day")
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
  //.filter($"Datestamp" >= (date_sub(current_timestamp(), 2) cast StringType))
  .filter($"unixTimestamp" > (unix_timestamp() - lit(86400)))

// COMMAND ----------

val MetricAggMonth = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Month")
  .withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
  //.filter($"YearMonth" >= substring((date_sub(current_timestamp(), 30) cast StringType),1,7))
  .filter($"unixTimestamp" > (unix_timestamp() - lit(2678400)))


// COMMAND ----------

val MetricAggHour = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Hour")
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
 // .filter(substring($"YearMonthDayHour",1,10) >= (date_sub(current_timestamp(), 30) cast StringType))
  .filter($"unixTimestamp" > (unix_timestamp() - lit(86400)))

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

val DfMetricAggDayCosmos = MetricAggDay.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

import org.apache.spark.sql.SaveMode
DfMetricAggDayCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByDay)

// COMMAND ----------

val DfMetricAggMonthCosmos = MetricAggMonth.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricAggMonthCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

val DfMetricAggHourCosmos = MetricAggHour.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricAggHourCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)