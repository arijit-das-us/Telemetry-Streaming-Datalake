// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._

// COMMAND ----------

// Configure the connection to your collection in Cosmos DB.
// Please refer to https://github.com/Azure/azure-cosmosdb-spark/wiki/Configuration-references
// for the description of the available configurations.
val configMap = Map(
  "Endpoint" -> "https://bigdata-kite-int.documents.azure.com:443/",
  "Masterkey" -> "3gK0zDd4znjmAleQQ0hhCEFtlGs1GRZBohkx5TjjdIfQ5edd4LhOiz7u8QOkaFsk9JBE0xXOvoxuKK9hkjwAHw==",
  "Database" -> "metric-live-dump",
  "Collection" -> "Metric-dump-AggByDay",
  "preferredRegions" -> "West US 2",
  "Upsert" -> "true",
  "WritingBatchSize" -> "10000")
val config = Config(configMap)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val DfAggByDay = spark.read.parquet(basePath + "BigDataTable/Metric-gold-Agg-Day").select("Datestamp","RemoteId", "Kymeta_metricId","sum_value","avg_value","min_value","max_value").withColumn("sum_value", $"sum_value".cast(DoubleType)).withColumn("avg_value", $"avg_value".cast(DoubleType)).withColumn("min_value", $"min_value".cast(DoubleType)).withColumn("max_value", $"max_value".cast(DoubleType)).coalesce(1)

// COMMAND ----------

display(DfAggByDay)

// COMMAND ----------

import org.apache.spark.sql.SaveMode
DfAggByDay.write.mode(SaveMode.Overwrite).cosmosDB(config)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val dfLiveMetricStream = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unix_timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
  .repartition(1)

// COMMAND ----------

display(dfLiveMetricStream)

// COMMAND ----------

// write the dataset to Cosmos DB.
//CosmosDBSpark.save(dfLiveMetricStream, config)

import org.apache.spark.sql.SaveMode
dfLiveMetricStream.write.mode(SaveMode.Overwrite).cosmosDB(config)

// COMMAND ----------

// Read the data written by the previous cell back.
val dataInCosmosDb = spark.sqlContext.read.cosmosDB(config)
display(dataInCosmosDb.orderBy(col("value")))