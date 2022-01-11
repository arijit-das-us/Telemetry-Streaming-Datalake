// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
val MetricLatestRaw = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-latest")
  .filter($"unixTimestamp" > (unix_timestamp() - lit(2678400)))
  .withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
 // .filter(substring($"YearMonthDayHour",1,10) >= (date_sub(current_timestamp(), 30) cast StringType))
 

// COMMAND ----------

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider
// Configure the connection to your collection in Cosmos DB.
// Please refer to https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references
// for the description of the available configurations.


val configMapAggLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggLatestTimeStamp = Config(configMapAggLatestTimeStamp)


// COMMAND ----------

val DfMetricLatestRawCosmos = MetricLatestRaw.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString").withColumn("valueInDouble", $"valueInDouble".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricLatestRawCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatestTimeStamp)