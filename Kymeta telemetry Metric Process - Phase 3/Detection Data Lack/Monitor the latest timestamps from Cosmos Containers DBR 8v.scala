// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
 
val cal1 = Calendar.getInstance();
cal1.add(Calendar.DAY_OF_YEAR, - 1);
val oneDayAgoDateTimeInMillis = cal1.toInstant.toEpochMilli
val oneDayAgoUnixTime = oneDayAgoDateTimeInMillis / 1000L

val cal3 = Calendar.getInstance();
cal3.add(Calendar.HOUR_OF_DAY, - 1);
val oneHoursAgoDateTimeInMillis = cal3.toInstant.toEpochMilli
val oneHoursAgoUnixTime = oneHoursAgoDateTimeInMillis / 1000L

val cal4 = Calendar.getInstance();
cal4.add(Calendar.DAY_OF_YEAR, - 30);
val oneMonthAgoDateTimeInMillis = cal4.toInstant.toEpochMilli
val oneMonthAgoUnixTime = oneMonthAgoDateTimeInMillis / 1000L

// COMMAND ----------

var sQueryLiveRaw = "SELECT c.metricProviderId, max(c.unixTimestamp) as maxUnixTimestamp FROM c where c.unixTimestamp >= " + oneHoursAgoUnixTime + " group by c.metricProviderId"
val configMapRawLive = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseRaw,
  "spark.cosmos.container" -> cdbCollectionLiveRaw,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
   "spark.cosmos.read.customQuery" -> sQueryLiveRaw
  )

// COMMAND ----------

// Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cdbEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cdbMasterkey)


// COMMAND ----------

val LiveRawMaxTSCosmos = 
spark.read.format("cosmos.oltp").options(configMapRawLive)
.option("spark.cosmos.read.inferSchema.enabled", "true")
//.groupBy("metricProviderId")
//.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
 .load()
.withColumn("container",lit("RAW"))
.select("container","metricProviderId","maxUnixTimestamp")
.groupBy("metricProviderId","container")
.agg(max("maxUnixTimestamp") as "MaxTimestamp")


//display(LatestTSCosmos)

// COMMAND ----------

//display(LiveRawMaxTSCosmos)

// COMMAND ----------

var sQueryLatestRaw = "SELECT c.metricProviderId, max(c.unixTimestamp) as maxUnixTimestamp FROM c where c.unixTimestamp >= " + oneHoursAgoUnixTime + " group by c.metricProviderId"
val configMapLatest = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseRaw,
  "spark.cosmos.container" -> cdbCollectionAggLatestTime,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
   "spark.cosmos.read.customQuery" -> sQueryLatestRaw
  )

// COMMAND ----------

val LatestMaxTSCosmos = 
spark.read.format("cosmos.oltp").options(configMapLatest)
.option("spark.cosmos.read.inferSchema.enabled", "true")
//.groupBy("metricProviderId")
//.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
 .load()
.withColumn("container",lit("LATEST"))
.select("container","metricProviderId","maxUnixTimestamp")
.groupBy("metricProviderId","container")
.agg(max("maxUnixTimestamp") as "MaxTimestamp")


//display(LatestTSCosmos)

// COMMAND ----------

//display(LatestMaxTSCosmos)

// COMMAND ----------

var sQueryAggDay = "SELECT c.metricProviderId, max(c._ts) as maxUnixTimestamp FROM c where c.unixTimestamp >= " + oneDayAgoUnixTime + " group by c.metricProviderId"
val configMapAggDay = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseAgg,
  "spark.cosmos.container" -> cdbCollectionAggByDay,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
   "spark.cosmos.read.customQuery" -> sQueryAggDay
  )

// COMMAND ----------

val AggDayMaxTSCosmos = 
spark.read.format("cosmos.oltp").options(configMapAggDay)
.option("spark.cosmos.read.inferSchema.enabled", "true")
.option("spark.cosmos.read.inferSchema.includeTimestamp", "true")
//.groupBy("metricProviderId")
//.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
 .load()
.withColumn("container",lit("AGGDAY"))
.select("container","metricProviderId","maxUnixTimestamp")
.groupBy("metricProviderId","container")
.agg(max("maxUnixTimestamp") as "MaxTimestamp")


//display(LatestTSCosmos)

// COMMAND ----------

//display(AggDayMaxTSCosmos)

// COMMAND ----------

var sQueryAggHour = "SELECT c.metricProviderId, max(c._ts) as maxUnixTimestamp FROM c where c.unixTimestamp >= " + oneHoursAgoUnixTime + " group by c.metricProviderId"
val configMapAggHour = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseAgg,
  "spark.cosmos.container" -> cdbCollectionAggByHour,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
   "spark.cosmos.read.customQuery" -> sQueryAggHour
  )

// COMMAND ----------

val AggHourMaxTSCosmos = 
spark.read.format("cosmos.oltp").options(configMapAggHour)
.option("spark.cosmos.read.inferSchema.enabled", "true")
.option("spark.cosmos.read.inferSchema.includeTimestamp", "true")
//.groupBy("metricProviderId")
//.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
 .load()
.withColumn("container",lit("AGGHOUR"))
.select("container","metricProviderId","maxUnixTimestamp")
.groupBy("metricProviderId","container")
.agg(max("maxUnixTimestamp") as "MaxTimestamp")


//display(LatestTSCosmos)

// COMMAND ----------

//display(AggHourMaxTSCosmos)

// COMMAND ----------

var sQueryAggMonth = "SELECT c.metricProviderId, max(c._ts) as maxUnixTimestamp FROM c where c.unixTimestamp >= " + oneMonthAgoUnixTime + " group by c.metricProviderId"
val configMapAggMonth = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseAgg,
  "spark.cosmos.container" -> cdbCollectionAggByMonth,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
   "spark.cosmos.read.customQuery" -> sQueryAggMonth
  )

// COMMAND ----------

val AggMonthMaxTSCosmos = 
spark.read.format("cosmos.oltp").options(configMapAggMonth)
.option("spark.cosmos.read.inferSchema.enabled", "true")
.option("spark.cosmos.read.inferSchema.includeTimestamp", "true")
//.groupBy("metricProviderId")
//.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
 .load()
.withColumn("container",lit("AGGMONTH"))
.select("container","metricProviderId","maxUnixTimestamp")
.groupBy("metricProviderId","container")
.agg(max("maxUnixTimestamp") as "MaxTimestamp")


//display(LatestTSCosmos)

// COMMAND ----------

//display(AggMonthMaxTSCosmos)

// COMMAND ----------

val MaxTSCosmos = LiveRawMaxTSCosmos
.union(LatestMaxTSCosmos)
.union(AggDayMaxTSCosmos)
.union(AggHourMaxTSCosmos)
.union(AggMonthMaxTSCosmos)
.withColumn("Name", when($"metricProviderId" === 2, "INTELSAT").when($"metricProviderId" === 1, "IDIRECT").when($"metricProviderId" === 3, "EVO").when($"metricProviderId" === 5, "ASM").when($"metricProviderId" === 4, "CUBIC").when($"metricProviderId" === 6, "PEPLINK"))
.withColumnRenamed("MaxTimestamp","LatestTimestamp")
.withColumn("Threshold", when(col("metricProviderId") === 4, 86400).when(col("metricProviderId") === 6, 86400).when(col("metricProviderId") === 1, 15552000).when(col("metricProviderId") === 2, 15552000).otherwise("3600"))


// COMMAND ----------

//display(MaxTSCosmos)

// COMMAND ----------

val RddMap = MaxTSCosmos
.withColumn("value",to_json(struct($"container", $"Name", $"LatestTimestamp", $"Threshold")))
.withColumn("key",concat($"container", lit("_"), $"Name"))
.select("key","value")
.map{x =>
  (x.getString(0), x.getString(1))
}
.rdd

// COMMAND ----------

import com.redislabs.provider.redis._

val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

// COMMAND ----------

sc.toRedisHASH(RddMap, "Databrick:KITEMaxTimestamp")(redisConfig)