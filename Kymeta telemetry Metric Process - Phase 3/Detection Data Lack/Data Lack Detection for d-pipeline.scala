// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
 

val cal1 = Calendar.getInstance
def currentTime = new java.util.Date();
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
val toDay = dateFormat.format(cal1.getTime)
val cal2 = Calendar.getInstance
cal2.add(Calendar.DATE, -1)
val yesterDay = dateFormat.format(cal2.getTime)
val cal3 = Calendar.getInstance();
cal3.add(Calendar.HOUR_OF_DAY, - 3);
val threeHoursBack = dateFormat.format(cal3.getTime)
val threeHoursAgoDateTimeInMillis = cal3.toInstant.toEpochMilli
val threeHoursAgoUnixTime = threeHoursAgoDateTimeInMillis / 1000L
 

// COMMAND ----------



// COMMAND ----------

val LatestTSIntelsatBronze = spark.sql("select max(timestamp) as MaxTimestamp from IntelsatusageBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(2))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSHubusageBronze = spark.sql("select max(timestamp) as MaxTimestamp from HubusageBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(1))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSHubstatsBronze = spark.sql("select max(timestamp) as MaxTimestamp from HubstatsBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(1))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSHubstatusBronze = spark.sql("select max(timestamp) as MaxTimestamp from HubstatusBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(1))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSiDirectBronze = LatestTSHubusageBronze.union(LatestTSHubstatsBronze).union(LatestTSHubstatusBronze)
.groupBy("metricProviderId","Stage")
.agg(max("MaxTimestamp").alias("MaxTimestamp"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSEvoBronze = spark.sql("select max(timestamp) as MaxTimestamp from EvoBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(3))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSCubicBronze = spark.sql("select max(timestamp) as MaxTimestamp from CubicBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(4))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSAsmBronze = spark.sql("select max(timestamp) as MaxTimestamp from AsmBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(5))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")
val LatestTSPeplinkBronze = spark.sql("select max(timestamp) as MaxTimestamp from PeplinkBronze where DateStamp >='" + threeHoursBack + "'")
.withColumn("metricProviderId",lit(6))
.withColumn("Stage",lit("BRONZE"))
.select("Stage","metricProviderId","MaxTimestamp")

val LatestTSBronze = LatestTSIntelsatBronze
.union(LatestTSiDirectBronze)
.union(LatestTSEvoBronze)
.union(LatestTSCubicBronze)
.union(LatestTSAsmBronze)
.union(LatestTSPeplinkBronze)

// COMMAND ----------

val LatestTSSilver = spark.sql("select metricProviderId, max(unix_timestamp) as MaxTimestamp from MetricSilver2 where DateStamp >='" + threeHoursBack + "' group by metricProviderId")
//.filter($"metricProviderId" === 2)
.withColumn("Stage",lit("SILVER"))
.select("Stage","metricProviderId","MaxTimestamp")


// COMMAND ----------

val LatestTSGold = spark.sql("select metricProviderId, max(unixTimestamp) as MaxTimestamp from MetricGold where DateStamp >='" + threeHoursBack + "' group by metricProviderId")
//.filter($"metricProviderId" === 2)
.withColumn("Stage",lit("GOLD"))
.select("Stage","metricProviderId","MaxTimestamp")

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config

var sQuery = "SELECT c.metricProviderId, max(c.unixTimestamp) as maxUnixTimestamp FROM c where c.unixTimestamp > " + threeHoursAgoUnixTime + " group by c.metricProviderId"

val configMap = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionLiveRaw,
  "preferredRegions" -> cdbRegions,
 // "query_maxdegreeofparallelism" -> "1",
  "query_custom" -> sQuery
  )
val config = Config(configMap)


// COMMAND ----------

val LatestTSCosmos = spark.sqlContext.read.cosmosDB(config)
.groupBy("metricProviderId")
.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
.withColumn("Stage",lit("COSMOS"))
.select("Stage","metricProviderId","MaxTimestamp")

//display(LatestTSCosmos)

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