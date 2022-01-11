// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Step 04
// MAGIC 1. read messages from Kafka topic intelsatusage
// MAGIC 2. get the NetworkProfile->ID and join with sspc table by rowkey to get the usage Name
// MAGIC 3. get the terminal ID and join terminal table by rowkey to get sat router ID
// MAGIC 4. join router table by rowkey and sat router ID
// MAGIC 5. get the model serial number
// MAGIC 6. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 7. join metricmapping table by primaryRawSymbol and mappingType by the usage->byteReceived and byteTransmitted and usage type (USAGE_NMS, USAGE_MGMT, USAGE_DATA)
// MAGIC 8. get the Kymeta metric Id from above join from metric mapping table
// MAGIC 9. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to delta table

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

//spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", false)
//spark.conf.set("spark.sql.streaming.aggregation.stateFormatVersion", 1)

// COMMAND ----------

// MAGIC %md 
// MAGIC #Custom Metrics

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val factCustommetrics = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/custommetrics")

display(factCustommetrics.select($"metricId").dropDuplicates())

// COMMAND ----------

val streamingInputDFGold = 
spark.readStream
  .format("delta")
  .option("startingVersion", "1199578")
  //.option("startingTimestamp", "2020-10-01")
  .load(basePath + "DeltaTable/Metric-gold-raw")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val dfcustomMetricSUM = streamingInputDFGold.join(factCustommetrics).where(streamingInputDFGold("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "SUM")) 
.select("unixTimestamp","dateStamp", "valueInDouble", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType","currentTimestamp")
.withColumn("TimestampYYMMddHHmmss", from_unixtime($"currentTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

val dfCustomMetricsAggSumUp = dfcustomMetricSUM
.withWatermark("TimestampYYMMddHHmmss", "60 seconds")
.groupBy($"unixTimestamp",$"RemoteId", $"metricProviderId", $"categoryId", $"metricId", $"TimestampYYMMddHHmmss")
.agg((sum($"valueInDouble")).cast(DecimalType(30,15)).alias("valueInDouble"))
//dfIntelsatUsageAddup.printSchema

// COMMAND ----------

val dfCustomMetricStreamSUM = dfCustomMetricsAggSumUp
.withColumn("valueInString", lit(null).cast(StringType))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")
.withColumn("Datestamp", from_unixtime($"unixTimestamp","yyyy-MM-dd"))
.withColumn("currentTimestamp", unix_timestamp())
.withColumn("aggregationType", lit("SUM"))


// COMMAND ----------

/*
val dfCustomMetricsSumNormalized = dfCustomMetricStreamSUM
.withColumn("valueInDouble",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, (col("valueInDouble") * 1000).cast(DecimalType(30,15))).otherwise(col("valueInDouble")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, 9003).otherwise(col("kymetaMetricId")))
       */

// COMMAND ----------


val dfCustomMetricsSumNormalized = dfCustomMetricStreamSUM
.withColumn("valueInDouble",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, (col("valueInDouble") * 1000).cast(DecimalType(30,15))).otherwise(col("valueInDouble")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9005 && col("metricProviderID") === 3, 9003).otherwise(col("kymetaMetricId")))
.withColumn("valueInDouble",
       when((col("kymetaMetricId") === 9008 || col("kymetaMetricId") === 9009) && col("metricProviderID") === 3, ((col("valueInDouble")/60) * 1000).cast(DecimalType(30,15))).when((col("kymetaMetricId") === 9008 || col("kymetaMetricId") === 9009) && (col("metricProviderID") === 1|| col("metricProviderID") === 2), (col("valueInDouble")/300).cast(DecimalType(30,15))).otherwise(col("valueInDouble")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9008, 9006).otherwise(col("kymetaMetricId")))
.withColumn("kymetaMetricId",
       when(col("kymetaMetricId") === 9009, 9007).otherwise(col("kymetaMetricId")))

// COMMAND ----------

//display(dfCustomMetricsSumNormalized)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldCustomPool1")

val dfExplodedQuery2 =  
dfCustomMetricsSumNormalized
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.writeStream
.queryName("GoldCustomQuery1")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/metric-custom-sum-07222021_3")
.option("path",basePath + "DeltaTable/Metric-gold-custom-sum")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

val streamingInputDFCustomSUM = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-custom-sum")

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "GoldPool2")

val dfExplodedQuery3 = 
streamingInputDFCustomSUM
.select("elementId", "unixTimestamp", "dateStamp", "metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId", "aggregationType", "currentTimestamp")
.writeStream
.queryName("GoldCustomQuery2")
.format("delta")
.partitionBy("dateStamp")
//.option("mergeSchema", "true")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/customsum-to-gold-raw")
.option("path",basePath + "DeltaTable/Metric-gold-raw")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("15 seconds")).start()

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFGold = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))
  .filter($"valueInDouble" =!= 0 && ($"aggregationType" === "SUM" || $"aggregationType" === "AVG") && $"metricProviderId" === 3 && $"kymetaMetricId".isin(9001,9004,9009,9002,9003,9005,9008) )
  .select("dateStamp", "unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")
  .dropDuplicates("dateStamp", "unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")


// COMMAND ----------

InputDFGold
.write
.format("delta")
.partitionBy("Datestamp")
.mode("overwrite")
.save(basePath + "DeltaTable/testHistory/Gold-evo-all-9000-dedup") 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select *, ROW_NUMBER() OVER (Partition By dateStamp, unixTimestamp, remoteId, kymetaMetricId Order By currentTimestamp desc) as rn  from MetricGold --where rn > 1
// MAGIC where valueInDouble != 0 and metricProviderId = 3 and kymetaMetricId in (9001,9004,9009,9002,9003,9005,9008) and dateStamp >= '2021-07-01' and dateStamp < '2021-07-30') t1 where rn > 1

// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
//import com.azure.cosmos.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val goldEvoDf = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/testHistory/Gold-evo-all-9000-dedup")

//display(goldEvoDf)

// COMMAND ----------

val InputDFDayDelimeter = goldEvoDf
  //.filter($"unixTimestamp" >= 1625097600 && $"unixTimestamp" < 1627257600)
  .filter($"unixTimestamp" < 1625097600)
  .withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")
//display(InputDFDayDelimeter.orderBy($"unixTimestamp" desc))

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFDay = InputDFDayDelimeter.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------

val configMapAggByDay = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByDay,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByDay = Config(configMapAggByDay)

// COMMAND ----------

val dfAggDayMetricCosmos = aggregatesDFDay
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

println(dfAggDayMetricCosmos.count)

// COMMAND ----------

//display(dfAggDayMetricCosmos.orderBy($"unixTimestamp" desc))

// COMMAND ----------

import org.apache.spark.sql.SaveMode
dfAggDayMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByDay)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFMonthDelimeter = 
goldEvoDf
  //.filter($"unixTimestamp" >= 1625097600 && $"unixTimestamp" < 1627257600)
  .filter($"unixTimestamp" < 1625097600)
  .withColumn("unixTimestamp", unix_timestamp(substring($"dateStamp",0,7),"yyyy-MM"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")
//display(InputDFMonthDelimeter.orderBy($"unixTimestamp" desc))


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFMonth = InputDFMonthDelimeter.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId","categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------

val configMapAggByMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByMonth = Config(configMapAggByMonth)

// COMMAND ----------

val dfAggMonthMetricCosmos = aggregatesDFMonth
.withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

display(dfAggMonthMetricCosmos)

// COMMAND ----------

dfAggMonthMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFHourDelimeter = 
goldEvoDf
  //.filter($"unixTimestamp" >= 1625097600 && $"unixTimestamp" < 1627257600)
  .filter($"unixTimestamp" < 1625097600)
  .withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM-dd HH"),"yyyy-MM-dd HH"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")
//display(InputDFHourDelimeter.orderBy($"unixTimestamp" desc))


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFHour = InputDFHourDelimeter.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
//import com.azure.cosmos.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val configMapAggByHour = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByHour,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByHour = Config(configMapAggByHour)

// COMMAND ----------

val dfAggHourMetricCosmos = aggregatesDFHour
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

println(dfAggHourMetricCosmos.count)

// COMMAND ----------

display(dfAggHourMetricCosmos.orderBy($"unixTimestamp" desc))

// COMMAND ----------

dfAggHourMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByHour)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFGold = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
//  .withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))
//  .filter($"unixTimestamp" >= 1625097600 && $"unixTimestamp" < 1627776000 && $"valueInDouble" =!= 0 && ($"aggregationType" === "SUM" || $"aggregationType" === "AVG") && $"metricProviderId" === 3 && $"kymetaMetricId".isin(9001,9004,9009,9002,9003,9005,9008) )
  .filter($"dateStamp".startsWith("2021-07") && $"valueInDouble" =!= 0 && ($"aggregationType" === "SUM" || $"aggregationType" === "AVG") && $"metricProviderId" === 3 && $"kymetaMetricId".isin(9001,9004,9009,9002,9003,9005,9008) )
  .select("dateStamp", "unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")
  .dropDuplicates("dateStamp", "unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFMonthDelimeter = 
InputDFGold
  //.filter($"unixTimestamp" >= 1625097600 && $"unixTimestamp" < 1627257600)
 // .filter($"unixTimestamp" < 1625097600)
  .withColumn("unixTimestamp", unix_timestamp(substring($"dateStamp",0,7),"yyyy-MM"))
  .select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")
//display(InputDFMonthDelimeter.orderBy($"unixTimestamp" desc))


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFMonth = InputDFMonthDelimeter.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId","categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )
.withColumn("currentTimestamp", unix_timestamp())


// COMMAND ----------

import org.joda.time._
import org.joda.time.format._

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Config

import org.apache.spark.sql.functions._
//import com.azure.cosmos.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val configMapAggByMonth = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseAgg,
  "Collection" -> cdbCollectionAggByMonth,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configAggByMonth = Config(configMapAggByMonth)

// COMMAND ----------

val dfAggMonthMetricCosmos = aggregatesDFMonth
.withColumn("id",concat(col("remoteId"),lit('|'),col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.select("id","unixTimestamp","remoteId","kymetaMetricId","metricProviderId","categoryId","sumValue","avgValue","minValue","maxValue")
.withColumn("sumValue", $"sumValue".cast(DoubleType))
.withColumn("avgValue", $"avgValue".cast(DoubleType))
.withColumn("minValue", $"minValue".cast(DoubleType))
.withColumn("maxValue", $"maxValue".cast(DoubleType))
.coalesce(32)

// COMMAND ----------

display(dfAggMonthMetricCosmos)

// COMMAND ----------

dfAggMonthMetricCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configAggByMonth)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select *, ROW_NUMBER() OVER (Partition By dateStamp, unixTimestamp, remoteId, kymetaMetricId Order By currentTimestamp desc) as rn  from MetricGold --where rn > 1
// MAGIC where valueInDouble != 0 and metricProviderId = 3 
// MAGIC and kymetaMetricId in (9001,9004,9009,9002,9003,9005,9008) 
// MAGIC and dateStamp >= '2021-07-01' and dateStamp < '2021-07-30'
// MAGIC ) t1 where rn > 1

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE into MetricGold as target
// MAGIC USING ( 
// MAGIC select * from (select *, ROW_NUMBER() OVER (Partition By dateStamp, unixTimestamp, remoteId, kymetaMetricId Order By currentTimestamp desc) as rn  from MetricGold --where rn > 1
// MAGIC where valueInDouble != 0 and metricProviderId = 3 
// MAGIC and kymetaMetricId in (9001,9004,9009,9002,9003,9005,9008) 
// MAGIC and dateStamp >= '2021-07-01' and dateStamp < '2021-07-30'
// MAGIC ) t1 where rn > 1
// MAGIC ) as source
// MAGIC ON source.dateStamp = target.dateStamp and source.unixTimestamp = target.unixTimestamp and source.remoteId = target.remoteId and source.kymetaMetricId = target.kymetaMetricId and source.metricProviderId = target.metricProviderId and source.currentTimestamp = target.currentTimestamp
// MAGIC WHEN MATCHED THEN DELETE