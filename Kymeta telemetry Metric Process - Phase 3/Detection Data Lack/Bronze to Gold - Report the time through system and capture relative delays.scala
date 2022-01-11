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
cal1.setTime(currentTime);
val toDay = dateFormat.format(cal1.getTime)

val cal3 = Calendar.getInstance();
cal3.add(Calendar.HOUR_OF_DAY, - 1);
val oneHoursAgoDateTimeInMillis = cal3.toInstant.toEpochMilli
val oneHoursAgoUnixTime = oneHoursAgoDateTimeInMillis / 1000L


// COMMAND ----------

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")

// COMMAND ----------

// Reset the output  table
//val metric_time_trhough_delays = basePath + "DeltaTable/Metric-time-through-system-delays"

//Seq.empty[(String,Long,Long,String,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal,BigDecimal)].toDF("DateStamp", "currentTimestamp", "metricProviderId", "ProviderName", "AvgDelayBronze","MaxDelayBronze","MinDelayBronze","AvgDelayGold","MaxDelayGold","MinDelayGold", "AvgDelayCosmos","MaxDelayCosmos","MinDelayCosmos" )
//.write
//.format("delta")
//.mode("append")
//.partitionBy("DateStamp")
//.option("path",metric_time_trhough_delays)
//.save

// COMMAND ----------

//%sql
//create database KITE

// COMMAND ----------

//spark.sql(s"""
//CREATE TABLE IF NOT EXISTS KITE.MonitorTimeThroughSystem 
//  USING DELTA 
//  OPTIONS (path = "$metric_time_trhough_delays")
//  """)

// COMMAND ----------

var sQueryLiveGoldRaw = spark.sql("SELECT dateStamp, metricProviderId, (currentTimestamp - unixTimestamp) as GoldUnixTimeDelays FROM MetricGold where dateStamp = '" + toDay + "' and currentTimestamp >= " + oneHoursAgoUnixTime)


// COMMAND ----------

val LatestTSIntelsatBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from IntelsatusageBronze where DateStamp ='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(2))

val LatestTSHubusageBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from HubusageBronze where DateStamp >='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(1))

val LatestTSHubstatsBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from HubstatsBronze where DateStamp >='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(1))

val LatestTSHubstatusBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from HubstatusBronze where DateStamp >='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(1))

val LatestTSEvoBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from EvoBronze where DateStamp >='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(3))

val LatestTSCubicBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from CubicBronze where DateStamp >='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(4))

val LatestTSAsmBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from AsmBronze where DateStamp ='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(5))

val LatestTSPeplinkBronze = spark.sql("select Datestamp, (unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') - timestamp) as BronzeUnixTimeDelays from PeplinkBronze where Datestamp ='" + toDay + "' and unix_timestamp(substring(LoadTime,0,19), 'yyyy-MM-dd HH:mm:ss') >= " + oneHoursAgoUnixTime)
.withColumn("metricProviderId",lit(6))


val DelayBronzeDf = LatestTSIntelsatBronze
.union(LatestTSHubusageBronze)
.union(LatestTSHubstatsBronze)
.union(LatestTSHubstatusBronze)
.union(LatestTSEvoBronze)
.union(LatestTSCubicBronze)
.union(LatestTSAsmBronze)
.union(LatestTSPeplinkBronze)

// COMMAND ----------

var sQueryLiveCosmosRaw = "SELECT c.unixTimestamp, c.metricProviderId, (c._ts - c.unixTimestamp) as CosmosUnixTimeDelays FROM c where c.unixTimestamp >= " + oneHoursAgoUnixTime
val configMapRawLive = Map(
  "spark.cosmos.accountEndpoint" -> cdbEndpoint,
  "spark.cosmos.accountKey" -> cdbMasterkey,
  "spark.cosmos.database" -> cdbDatabaseRaw,
  "spark.cosmos.container" -> cdbCollectionLiveRaw,
  "spark.cosmos.preferredRegionsList" -> cdbRegions,
   "spark.cosmos.read.customQuery" -> sQueryLiveCosmosRaw
  )

// Configure Catalog Api to be used
spark.conf.set("spark.sql.catalog.cosmosCatalog", "com.azure.cosmos.spark.CosmosCatalog")
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountEndpoint", cdbEndpoint)
spark.conf.set("spark.sql.catalog.cosmosCatalog.spark.cosmos.accountKey", cdbMasterkey)


// COMMAND ----------

val sQueryDelaysCosmosAgg = 
spark.read.format("cosmos.oltp").options(configMapRawLive)
.option("spark.cosmos.read.inferSchema.enabled", "true")
//.groupBy("metricProviderId")
//.agg(max("maxUnixTimestamp") as "MaxTimestamp")
//.filter($"metricProviderId" === 2)
 .load()
.filter($"unixTimestamp" <= unix_timestamp())
.select(from_unixtime($"unixTimestamp","yyyy-MM-dd").alias("Datestamp"),$"metricProviderId",$"CosmosUnixTimeDelays")
.groupBy("metricProviderId","dateStamp")
.agg((avg($"CosmosUnixTimeDelays")).alias("AvgDelayCosmos"), (min($"CosmosUnixTimeDelays")).alias("MinDelayCosmos"),(max($"CosmosUnixTimeDelays")).alias("MaxDelayCosmos") )

// COMMAND ----------

//display(sQueryLiveGoldRawCosmos)

// COMMAND ----------

val sQueryDelaysBrAgg = DelayBronzeDf.groupBy("metricProviderId","dateStamp")
.agg((avg($"BronzeUnixTimeDelays")).alias("AvgDelayBronze"), (min($"BronzeUnixTimeDelays")).alias("MinDelayBronze"),(max($"BronzeUnixTimeDelays")).alias("MaxDelayBronze") )

// COMMAND ----------

val sQueryDelaysGoldAgg = sQueryLiveGoldRaw.groupBy("metricProviderId","dateStamp")
.agg((avg($"GoldUnixTimeDelays")).alias("AvgDelayGold"), (min($"GoldUnixTimeDelays")).alias("MinDelayGold"),(max($"GoldUnixTimeDelays")).alias("MaxDelayGold") )

// COMMAND ----------

val GoldBronzeAggDf = sQueryDelaysBrAgg.join(sQueryDelaysGoldAgg, sQueryDelaysBrAgg("Datestamp") === sQueryDelaysGoldAgg("Datestamp") and sQueryDelaysBrAgg("metricProviderId") === sQueryDelaysGoldAgg("metricProviderId")).select(sQueryDelaysBrAgg("Datestamp"), sQueryDelaysBrAgg("metricProviderId"), sQueryDelaysBrAgg("AvgDelayBronze").cast(DecimalType(10,2)), sQueryDelaysBrAgg("MaxDelayBronze").cast(DecimalType(10,2)), sQueryDelaysBrAgg("MinDelayBronze").cast(DecimalType(10,2)), sQueryDelaysGoldAgg("AvgDelayGold").cast(DecimalType(10,2)), sQueryDelaysGoldAgg("MaxDelayGold").cast(DecimalType(10,2)), sQueryDelaysGoldAgg("MinDelayGold").cast(DecimalType(10,2)))

val GoldBronzeCosmosAggDf = GoldBronzeAggDf
.join(sQueryDelaysCosmosAgg, GoldBronzeAggDf("Datestamp") === sQueryDelaysCosmosAgg("Datestamp") and GoldBronzeAggDf("metricProviderId") === sQueryDelaysCosmosAgg("metricProviderId"))
.select(GoldBronzeAggDf("Datestamp"), GoldBronzeAggDf("metricProviderId"), GoldBronzeAggDf("AvgDelayBronze").cast(DecimalType(10,2)), GoldBronzeAggDf("MaxDelayBronze").cast(DecimalType(10,2)), GoldBronzeAggDf("MinDelayBronze").cast(DecimalType(10,2)), GoldBronzeAggDf("AvgDelayGold").cast(DecimalType(10,2)), GoldBronzeAggDf("MaxDelayGold").cast(DecimalType(10,2)), GoldBronzeAggDf("MinDelayGold").cast(DecimalType(10,2)), sQueryDelaysCosmosAgg("AvgDelayCosmos").cast(DecimalType(10,2)), sQueryDelaysCosmosAgg("MaxDelayCosmos").cast(DecimalType(10,2)), sQueryDelaysCosmosAgg("MinDelayCosmos").cast(DecimalType(10,2)))
.withColumn("ProviderName", when($"metricProviderId" === 2, "INTELSAT").when($"metricProviderId" === 1, "IDIRECT").when($"metricProviderId" === 3, "EVO").when($"metricProviderId" === 5, "ASM").when($"metricProviderId" === 4, "CUBIC").when($"metricProviderId" === 6, "PEPLINK"))
.withColumn("currentTimestamp", unix_timestamp())

// COMMAND ----------

//display(GoldBronzeCosmosAggDf)

// COMMAND ----------

import org.apache.spark.sql._
import io.delta.tables._
val deltaTableTimeThrough = DeltaTable.forName("KITE.MonitorTimeThroughSystem")


  deltaTableTimeThrough.as("t")
    .merge(
      GoldBronzeCosmosAggDf.as("s"), 
      "s.DateStamp = t.DateStamp and s.metricProviderId = t.metricProviderId")
    .whenMatched
      .updateExpr(
        Map(
          "t.metricProviderId" -> "s.metricProviderId",
          "t.ProviderName" -> "s.ProviderName",
           "t.AvgDelayBronze" -> "s.AvgDelayBronze",
           "t.MaxDelayBronze" -> "s.MaxDelayBronze",
           "t.MinDelayBronze" -> "s.MinDelayBronze",
           "t.AvgDelayGold" -> "s.AvgDelayGold",
           "t.MinDelayGold" -> "s.MinDelayGold",
           "t.MaxDelayGold" -> "s.MaxDelayGold",
            "t.AvgDelayCosmos" -> "s.AvgDelayCosmos",
           "t.MinDelayCosmos" -> "s.MinDelayCosmos",
           "t.MaxDelayCosmos" -> "s.MaxDelayCosmos",
           "t.dateStamp" -> "s.dateStamp",
            "t.currentTimestamp" -> "s.currentTimestamp"
           ))
    .whenNotMatched
      .insertExpr(
        Map(
          "t.metricProviderId" -> "s.metricProviderId",
          "t.ProviderName" -> "s.ProviderName",
           "t.AvgDelayBronze" -> "s.AvgDelayBronze",
           "t.MaxDelayBronze" -> "s.MaxDelayBronze",
           "t.MinDelayBronze" -> "s.MinDelayBronze",
           "t.AvgDelayGold" -> "s.AvgDelayGold",
           "t.MinDelayGold" -> "s.MinDelayGold",
           "t.MaxDelayGold" -> "s.MaxDelayGold",
           "t.AvgDelayCosmos" -> "s.AvgDelayCosmos",
           "t.MinDelayCosmos" -> "s.MinDelayCosmos",
           "t.MaxDelayCosmos" -> "s.MaxDelayCosmos", 
           "t.dateStamp" -> "s.dateStamp",
            "t.currentTimestamp" -> "s.currentTimestamp"
          ))
    .execute()


// COMMAND ----------

// MAGIC %sql
// MAGIC select * from KITE.MonitorTimeThroughSystem