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
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))
//display(dfLiveMetricStream)

// COMMAND ----------

//display(dfLiveMetricStream)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
//import org.apache.spark.sql.streaming.Trigger
//val writeDWOut = dfLiveMetricStream
//  .select("unix_timestamp", "metric", "value", "Kymeta_metricId", "metricProviderId","RemoteId")
//  .writeStream
//  .format("com.databricks.spark.sqldw")
//  .option("url", JDBC_URL_DATA_WAREHOUSE)
//  .option("tempDir", POLYBASE_TEMP_DIR)
//  .option("forwardSparkAzureStorageCredentials", "true")
//  .option("dbTable", "RawMetricLive")
//  .option("checkpointLocation", basePath + "SQLDW/_checkpoint/sqldw_MetricRawLive")
//  .outputMode("append")
//  .trigger(Trigger.ProcessingTime("300 seconds"))
//  .start()

//val streamingETLQuery = dfLiveMetricStream
//  .select("unix_timestamp", "metric", "value", "Kymeta_metricId", "metricProviderId","RemoteId")
//  .writeStream
//  .format("parquet")
//  .option("path", basePath + "BigDataTable/Metric-gold-live")
//  .trigger(Trigger.ProcessingTime("300 seconds"))
//  .option("checkpointLocation", basePath + "BigDataTable/_checkpoint/metric-gold-live")
//  .start()

// COMMAND ----------

val configMapAggLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> "5",
 // "ConnectionMaxPoolSize" -> "10",
  "CheckpointLocation" -> cdbCheckpointLatestMetric)
val configAggLatestTimeStamp = Config(configMapAggLatestTimeStamp)

// COMMAND ----------

val dfLatestMetricStreamCosmos = dfLiveMetricStream
.withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
.select("unixTimestamp", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))

// COMMAND ----------

//dfLiveMetricStreamCosmos.writeStream
//.format("parquet")
//.option("path", basePath + "Metric-gold-ToParquet")
//.option("checkpointLocation",basePath + "DeltaTable/_checkpoint/Metric-gold-ToParquet")
//.outputMode("append")
//.start()

// COMMAND ----------

// Write to Cosmos DB as stream
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
dfLatestMetricStreamCosmos.writeStream.format(classOf[CosmosDBSinkProvider].getName).outputMode("append").options(configMapAggLatestTimeStamp).option("checkpointLocation", cdbCheckpointLatestMetric)
//.trigger(Trigger.ProcessingTime("30 seconds"))
.start()

