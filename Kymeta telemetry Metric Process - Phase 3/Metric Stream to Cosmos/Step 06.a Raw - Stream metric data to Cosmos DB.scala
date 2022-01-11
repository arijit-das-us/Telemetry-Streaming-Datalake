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
  "spark.cosmos.write.strategy" -> "ItemOverwrite")

// COMMAND ----------

// Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cdbEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cdbMasterkey)


// COMMAND ----------


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
.format("cosmos.oltp")
.outputMode("append")
.options(configMapRawLive)
.option("checkpointLocation", cdbCheckpoint)
.start()

