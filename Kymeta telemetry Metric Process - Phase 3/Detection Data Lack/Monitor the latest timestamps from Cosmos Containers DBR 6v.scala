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
cal3.add(Calendar.HOUR_OF_DAY, - 1);
val oneHoursAgoDateTimeInMillis = cal3.toInstant.toEpochMilli
val oneHoursAgoUnixTime = threeHoursAgoDateTimeInMillis / 1000L

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

var sQueryLiveRaw = "SELECT c.metricProviderId, max(c.unixTimestamp) as maxUnixTimestamp FROM c where c.unixTimestamp >= " + oneHoursAgoUnixTime + " group by c.metricProviderId"

val configMapLiveRaw = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionLiveRaw,
  "preferredRegions" -> cdbRegions,
 // "query_maxdegreeofparallelism" -> "1",
  "query_custom" -> sQueryLiveRaw
  )
val configLiveRaw = Config(configMapLiveRaw)



// COMMAND ----------

val LiveRawMaxTSCosmos = spark.sqlContext.read.cosmosDB(configLiveRaw)
//.groupBy("metricProviderId")
//.agg(sum("SumOfSumValue") as "SumOfSumValue")
//.filter($"metricProviderId" === 2)
.withColumn("container",lit("RAW"))
.select("container","metricProviderId","maxUnixTimestamp")

// COMMAND ----------

display(LiveRawMaxTSCosmos)

// COMMAND ----------

spark.sqlContext.read.cosmosDB(config)
.groupBy("metricProviderId")
.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
.withColumn("container",lit("RAW"))
.select("container","metricProviderId","MaxTimestamp")


// COMMAND ----------

val LatestTS = LatestTSBronze
.union(LatestTSSilver)
.union(LatestTSGold)
.union(LatestTSCosmos)
.withColumn("Name", when($"metricProviderId" === 2, "INTELSAT").when($"metricProviderId" === 1, "IDIRECT").when($"metricProviderId" === 3, "EVO").when($"metricProviderId" === 5, "ASM").when($"metricProviderId" === 4, "CUBIC").when($"metricProviderId" === 6, "PEPLINK"))
.withColumnRenamed("MaxTimestamp","LatestTimestamp")


// COMMAND ----------

//display(LatestTS)

// COMMAND ----------

val RddMap = LatestTS
.withColumn("value",to_json(struct($"Stage", $"Name", $"LatestTimestamp")))
.withColumn("key",concat($"Stage", lit("_"), $"Name"))
.select("key","value")
.map{x =>
  (x.getString(0), x.getString(1))
}
.rdd

// COMMAND ----------

import com.redislabs.provider.redis._

val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

// COMMAND ----------

sc.toRedisHASH(RddMap, "Databrick:PipelineLatestUpdate")(redisConfig)