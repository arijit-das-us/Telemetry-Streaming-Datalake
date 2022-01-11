// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
 

val cal3 = Calendar.getInstance();
cal3.add(Calendar.HOUR_OF_DAY, - 3);
val threeHoursAgoDateTimeInMillis = cal3.toInstant.toEpochMilli
val threeHoursAgoUnixTime = threeHoursAgoDateTimeInMillis / 1000L
 


val cal4 = Calendar.getInstance
cal4.set(Calendar.DAY_OF_MONTH,0);
cal4.set(Calendar.HOUR_OF_DAY, 0);
cal4.set(Calendar.MINUTE, 0);
cal4.set(Calendar.SECOND, 0);
val FirstDayMonthInMillis = cal4.toInstant.toEpochMilli
val FirstDayMonthUnixTime = FirstDayMonthInMillis / 1000L

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val dfAggDayMetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Day")
  .filter($"kymetaMetricId" === 9003)
  .filter($"unixTimestamp" >= FirstDayMonthUnixTime)

// COMMAND ----------

val dfAggMonthMetric = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Month")
  .filter($"kymetaMetricId" === 9003)
  .filter($"unixTimestamp" >= FirstDayMonthUnixTime)

// COMMAND ----------

val SumMetricAggregatedByDay = dfAggDayMetric.groupBy("metricProviderId")
.agg((sum($"sumValue")).alias("SumOfSumValue"))
.withColumn("Stage",lit("AggregatedByDayInDatabrick"))
.select("Stage","metricProviderId","SumOfSumValue")

val SumMetricAggregatedByMonth = dfAggMonthMetric.groupBy("metricProviderId")
.agg((sum($"sumValue")).alias("SumOfSumValue"))
.withColumn("Stage",lit("AggregatedByMonthInDatabrick"))
.select("Stage","metricProviderId","SumOfSumValue")

// COMMAND ----------

//val SumMetricAggregatedByDay = spark.sql("select sum(sumValue) as SumOfSumValue from MetricAggregatedByDay where unixTimestamp >='" + FirstDayMonthUnixTime + "' and sumValue != 0")
//.withColumn("Stage",lit("AggregatedByDayInDatabrick"))
//.select("Stage","SumOfSumValue")

//val SumMetricAggregatedByMonth = spark.sql("select sum(sumValue) as SumOfSumValue from MetricAggregatedByMonth where unixTimestamp >='" + FirstDayMonthUnixTime + "' and sumValue != 0")
//.withColumn("Stage",lit("AggregatedByMonthInDatabrick"))
//.select("Stage","SumOfSumValue")


// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

var sQuery = "SELECT c.metricProviderId, sum(c.sumValue) as SumOfSumValue FROM c where c.kymetaMetricId = 9003 and c.unixTimestamp >= " + FirstDayMonthUnixTime + " group by c.metricProviderId"

val configMapDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
 // "query_maxdegreeofparallelism" -> "1",
  "query_custom" -> sQuery
  )
val configDay = Config(configMapDay)

val configMapMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
 // "query_maxdegreeofparallelism" -> "1",
  "query_custom" -> sQuery
  )
val configMonth = Config(configMapMonth)


// COMMAND ----------

val AggregatedByDayCosmos = spark.sqlContext.read.cosmosDB(configDay)
//.groupBy("metricProviderId")
//.agg(sum("SumOfSumValue") as "SumOfSumValue")
//.filter($"metricProviderId" === 2)
.withColumn("Stage",lit("AggregatedByDayinCOSMOS"))
.select("Stage","metricProviderId","SumOfSumValue")

val AggregatedByMonthCosmos = spark.sqlContext.read.cosmosDB(configMonth)
//.groupBy("metricProviderId")
//.agg(sum("SumOfSumValue") as "SumOfSumValue")
//.filter($"metricProviderId" === 2)
.withColumn("Stage",lit("AggregatedByMonthinCOSMOS"))
.select("Stage","metricProviderId","SumOfSumValue")

//display(LatestTSCosmos)

// COMMAND ----------

val AggregatedUsage = SumMetricAggregatedByDay
.union(SumMetricAggregatedByMonth)
.union(AggregatedByDayCosmos)
.union(AggregatedByMonthCosmos)

// COMMAND ----------

//display(AggregatedUsage.orderBy("metricProviderId"))

// COMMAND ----------

val RddMap = AggregatedUsage
.withColumn("value",to_json(struct($"Stage", $"metricProviderId", $"SumOfSumValue")))
.withColumn("key",concat($"Stage", lit("_"), $"metricProviderId"))
.select("key","value")
.map{x =>
  (x.getString(0), x.getString(1))
}
.rdd

// COMMAND ----------

import com.redislabs.provider.redis._

val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

// COMMAND ----------

sc.toRedisHASH(RddMap, "Databrick:PipelineAggregatedData")(redisConfig)

// COMMAND ----------

val SumMetricAggregatedByDay = dfAggDayMetric.groupBy("metricProviderId","remoteId")
.agg((sum($"sumValue")).alias("SumOfSumValue"))
.withColumn("Stage",lit("AggregatedByDayInDatabrick"))
.select("Stage","metricProviderId","remoteId","SumOfSumValue")

val SumMetricAggregatedByMonth = dfAggMonthMetric.groupBy("metricProviderId","remoteId")
.agg((sum($"sumValue")).alias("SumOfSumValue"))
.withColumn("Stage",lit("AggregatedByMonthInDatabrick"))
.select("Stage","metricProviderId","remoteId","SumOfSumValue")

// COMMAND ----------

var sQueryRemoteId = "SELECT c.metricProviderId, c.remoteId, sum(c.sumValue) as SumOfSumValue FROM c where c.kymetaMetricId = 9003 and c.unixTimestamp >= " + FirstDayMonthUnixTime + " group by c.metricProviderId, c.remoteId"

val configMapDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
 // "query_maxdegreeofparallelism" -> "1",
  "query_custom" -> sQueryRemoteId
  )
val configDay = Config(configMapDay)

val configMapMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
 // "query_maxdegreeofparallelism" -> "1",
  "query_custom" -> sQueryRemoteId
  )
val configMonth = Config(configMapMonth)

// COMMAND ----------

val AggregatedByDayCosmos = spark.sqlContext.read.cosmosDB(configDay)
//.groupBy("metricProviderId")
//.agg(sum("SumOfSumValue") as "SumOfSumValue")
//.filter($"metricProviderId" === 2)
.withColumn("Stage",lit("AggregatedByDayinCOSMOS"))
.select("Stage","metricProviderId","remoteId","SumOfSumValue")

val AggregatedByMonthCosmos = spark.sqlContext.read.cosmosDB(configMonth)
//.groupBy("metricProviderId")
//.agg(sum("SumOfSumValue") as "SumOfSumValue")
//.filter($"metricProviderId" === 2)
.withColumn("Stage",lit("AggregatedByMonthinCOSMOS"))
.select("Stage","metricProviderId","remoteId","SumOfSumValue")

//display(LatestTSCosmos)

// COMMAND ----------

val AggregatedUsageByRemote = SumMetricAggregatedByDay
.union(SumMetricAggregatedByMonth)
.union(AggregatedByDayCosmos)
.union(AggregatedByMonthCosmos)

// COMMAND ----------

//display(AggregatedUsageByRemote.orderBy("remoteId","metricProviderId","Stage"))

// COMMAND ----------

val RddMap = AggregatedUsageByRemote
.withColumn("value",to_json(struct($"Stage", $"metricProviderId", $"remoteId", $"SumOfSumValue")))
.withColumn("key",concat($"Stage", lit("_"), $"remoteId"))
.select("key","value")
.map{x =>
  (x.getString(0), x.getString(1))
}
.rdd

// COMMAND ----------

sc.toRedisHASH(RddMap, "Databrick:PipelineAggregatedDataByRemoteID")(redisConfig)