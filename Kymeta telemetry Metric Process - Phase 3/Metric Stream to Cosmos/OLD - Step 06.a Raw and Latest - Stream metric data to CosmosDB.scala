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


val dfLiveMetricStream = spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

val configMapRawLive = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionLiveRaw,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
 // "WritingBatchSize" -> cdbBatchSize,
  "WritingBatchSize" -> "10",
  "ConnectionMaxPoolSize" -> "50",
  "CheckpointLocation" -> cdbCheckpoint)
val configRawLive = Config(configMapRawLive)

// COMMAND ----------

val dfLiveMetricStreamCosmos = dfLiveMetricStream
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

// Write to Cosmos DB as stream
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "RawStreamPool1")

dfLiveMetricStreamCosmos
.writeStream
.queryName("RawStreamQuery1")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapRawLive)
.option("checkpointLocation", cdbCheckpoint)
.start()



// COMMAND ----------

/*
val dfLatestMetricStream = spark.readStream
  .format("delta")
  //.option("startingVersion", "236153")
  .load(basePath + "DeltaTable/Metric-gold-raw")
*/

// COMMAND ----------

/*
val dfLatestMetricStreamCosmos = dfLatestMetricStream
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("unixTimestamp", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
*/

// COMMAND ----------

/*
val dfLatestMetricHubStreamCosmos = dfLatestMetricStreamCosmos.filter($"metricProviderId" =!= 3)
//.filter($"metricProviderId" === 1 or $"metricProviderId" === 2)

val configMapAggLatestHub = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
 // "WritingBatchSize" -> "5",
//  "ConnectionMaxPoolSize" -> "5",
  "CheckpointLocation" -> cdbCPLatestMetriciDirect)
val configAggLatestHub = Config(configMapAggLatestHub)
*/

// COMMAND ----------

/*
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "LatestStreamPool1")

dfLatestMetricHubStreamCosmos
.writeStream
.queryName("LatestStreamQuery1")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapAggLatestHub)
.option("checkpointLocation", cdbCPLatestMetriciDirect)
.start()
*/

// COMMAND ----------

/*
val dfLatestMetricEvoStreamCosmos = dfLatestMetricStreamCosmos.filter($"metricProviderId" === 3 && $"kymetaMetricId".isin(40,75,108,116,120,229,9003))

val configMapAggLatestEvo = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> "5",
  "ConnectionMaxPoolSize" -> "5",
  "CheckpointLocation" -> cdbCPLatestMetricEvo)
val configAggLatestEvo = Config(configMapAggLatestEvo)
*/

// COMMAND ----------

/*
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "LatestStreamPool2")

dfLatestMetricEvoStreamCosmos
.writeStream
.queryName("LatestStreamQuery2")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapAggLatestEvo)
.option("checkpointLocation", cdbCPLatestMetricEvo)
.start()
*/

// COMMAND ----------

/*
val dfLatestMetricASMStreamCosmos = dfLatestMetricStreamCosmos.filter($"metricProviderId" === 5 )

val configMapAggLatestAsm = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> "5",
  "ConnectionMaxPoolSize" -> "5",
  "CheckpointLocation" -> cdbCPLatestMetricAsm)
val configAggLatestAsm = Config(configMapAggLatestAsm)
*/

// COMMAND ----------

/*
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "LatestStreamPool3")

dfLatestMetricASMStreamCosmos
.writeStream
.queryName("LatestStreamQuery3")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapAggLatestAsm)
.option("checkpointLocation", cdbCPLatestMetricAsm)
.start()
*/

// COMMAND ----------

/*
val dfLatestCubicPeplinkStreamCosmos = dfLatestMetricStreamCosmos.filter($"metricProviderId" === 4 || $"metricProviderId" === 6)

val configMapAggLatestCubicPep = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> "5",
  "ConnectionMaxPoolSize" -> "5",
  "CheckpointLocation" -> cdbCPLatestMetricCubicPep)
val configAggLatestCubicPep = Config(configMapAggLatestCubicPep)
*/

// COMMAND ----------

/*
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "LatestStreamPool4")

dfLatestCubicPeplinkStreamCosmos
.writeStream
.queryName("LatestStreamQuery4")
.format(classOf[CosmosDBSinkProvider].getName)
.outputMode("append")
.options(configMapAggLatestCubicPep)
.option("checkpointLocation", cdbCPLatestMetricCubicPep)
.start()
*/