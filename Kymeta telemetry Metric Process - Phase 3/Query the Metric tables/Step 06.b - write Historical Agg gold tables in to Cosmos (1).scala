// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val MetricAggDay = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/History-Agg-Day")
  .filter($"yearmonth" === "2020-07" || $"yearmonth" === "2020-08")
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
//  .filter($"Datestamp" >= (date_sub(current_timestamp(), 30) cast StringType))

// COMMAND ----------

println(MetricAggDay.count())

// COMMAND ----------

//display(MetricAggDay)

// COMMAND ----------

//val writeDWOut = MetricAggDay
//  .write
//  .format("com.databricks.spark.sqldw")
//  .option("url", JDBC_URL_DATA_WAREHOUSE)
//  .option("tempDir", POLYBASE_TEMP_DIR)
//  .option("forwardSparkAzureStorageCredentials", "true")
//  .option("dbTable", "metric_liveAggByDay")
//  .mode(SaveMode.Overwrite)
//  .save()

//MetricAggDay.write.mode("overwrite").parquet(basePath + "BigDataTable/Metric-gold-Agg-Day")

// COMMAND ----------

val MetricAggMonth = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/History-Agg-Month")
  .filter($"yearmonth" === "2020-07" || $"yearmonth" === "2020-08")
  .withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
 // .filter($"YearMonth" >= substring((date_sub(current_timestamp(), 30) cast StringType),1,7))

// COMMAND ----------

println(MetricAggMonth.count())

// COMMAND ----------

//display(MetricAggMonth)

// COMMAND ----------

//val writeDWOut = MetricAggMonth
//  .write
//  .format("com.databricks.spark.sqldw")
//  .option("url", JDBC_URL_DATA_WAREHOUSE)
//  .option("tempDir", POLYBASE_TEMP_DIR)
//  .option("forwardSparkAzureStorageCredentials", "true")
//  .option("dbTable", "metric_liveAggByMonth")
//  .mode(SaveMode.Overwrite)
//  .save()

//MetricAggMonth.write.mode("overwrite").parquet(basePath + "BigDataTable/Metric-gold-Agg-Month")

// COMMAND ----------

val MetricAggHour = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/History-Agg-Hour")
  .filter($"yearmonth" === "2020-07" || $"yearmonth" === "2020-08")
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
 // .filter(substring($"YearMonthDayHour",1,10) >= (date_sub(current_timestamp(), 30) cast StringType))

// COMMAND ----------

println(MetricAggHour.count())

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val MetricLatestRaw = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/History-Latest-Raw").filter($"metricProviderId" === 5)
  .withColumn("id",concat(col("remoteId"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
 // .filter(substring($"YearMonthDayHour",1,10) >= (date_sub(current_timestamp(), 30) cast StringType))

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val MetricHistoricalRaw = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/History-gold-raw").filter($"metricProviderId" === 5).filter($"yearMonth" === "2020-05")
  .withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
  .select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId"))).dropDuplicates("id")

// COMMAND ----------

println(MetricHistoricalRaw.count())

// COMMAND ----------

println(MetricLatestRaw.count())

// COMMAND ----------

val MetrichistoricalCustomJoin = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/History-gold-custom-join").filter($"metricProviderId" === 5)
 .withColumn("valueInDouble", $"valueInDouble".cast(DoubleType))
  .select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
  .withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId"))).dropDuplicates("id")

// COMMAND ----------

//println(MetricHistoricalRaw.count)
println(MetrichistoricalCustomJoin.count)

// COMMAND ----------

//MetricAggHour.printSchema

// COMMAND ----------

//display(MetricAggHour)

// COMMAND ----------

//val writeDWOut = MetricAggHour
//  .write
//  .format("com.databricks.spark.sqldw")
//  .option("url", JDBC_URL_DATA_WAREHOUSE)
//  .option("tempDir", POLYBASE_TEMP_DIR)
//  .option("forwardSparkAzureStorageCredentials", "true")
//  .option("dbTable", "metric_liveAggByHour")
//  .mode(SaveMode.Overwrite)
//  .save()

//MetricAggHour.write.mode("overwrite").parquet(basePath + "BigDataTable/Metric-gold-Agg-Hour")

// COMMAND ----------

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

val configMapAggLatestTimeStamp = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionAggLatestTime,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggLatestTimeStamp = Config(configMapAggLatestTimeStamp)

val configMapRawLive = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionLiveRaw,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize
 // "CheckpointLocation" -> cdbCheckpoint
)
val configRawLive = Config(configMapRawLive)


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

val DfMetricAggHourCosmos = 
MetricAggHour
//.filter($"yearmonth".startsWith("2020"))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricAggHourCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)

// COMMAND ----------

val DfMetricAggHourCosmos2019 = 
MetricAggHour
.filter($"yearmonth".startsWith("2019"))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricAggHourCosmos2019.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)

// COMMAND ----------

val DfMetricAggHourCosmos2019_09 = 
MetricAggHour
//.filter($"yearmonth".startsWith("2019-0"))
.filter($"yearmonth" === "2019-09")
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricAggHourCosmos2019_09.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)

// COMMAND ----------

val DfMetricAggHourCosmos2018 = 
MetricAggHour
.filter($"yearmonth".startsWith("2018"))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue").withColumn("sumValue", $"sumValue".cast(DoubleType)).withColumn("avgValue", $"avgValue".cast(DoubleType)).withColumn("minValue", $"minValue".cast(DoubleType)).withColumn("maxValue", $"maxValue".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricAggHourCosmos2018.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)

// COMMAND ----------

val DfMetricLatestRawCosmos = MetricLatestRaw.select("id","remoteId","kymetaMetricId","metricProviderId","categoryId","unixTimestamp","valueInDouble","valueInString").withColumn("valueInDouble", $"valueInDouble".cast(DoubleType)).coalesce(32)

// COMMAND ----------

DfMetricLatestRawCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggLatestTimeStamp)

// COMMAND ----------

MetricHistoricalRaw.coalesce(25).write.mode(SaveMode.Overwrite).cosmosDB(configRawLive)

// COMMAND ----------

MetrichistoricalCustomJoin.coalesce(10).write.mode(SaveMode.Overwrite).cosmosDB(configRawLive)

// COMMAND ----------

val InputDFStream = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")

// COMMAND ----------

display(InputDFStream)