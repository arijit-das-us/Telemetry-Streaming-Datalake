// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

// MAGIC 
// MAGIC %md 
// MAGIC #Process Metrics - ASM Metrics
// MAGIC 1. read messages from Kafka topic intelsatusage
// MAGIC 2. get the NetworkProfile->ID and join with sspc table by rowkey to get the usage Name
// MAGIC 3. get the terminal ID and join terminal table by rowkey to get sat router ID
// MAGIC 4. join router table by rowkey and sat router ID
// MAGIC 5. get the model serial number
// MAGIC 6. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 7. join metricmapping table by primaryRawSymbol and mappingType by the usage->byteReceived and byteTransmitted and usage type (USAGE_NMS, USAGE_MGMT, USAGE_DATA)
// MAGIC 8. get the Kymeta metric Id from above join from metric mapping table
// MAGIC 9. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to delta table
// MAGIC 9. stream the data (remote ID, Metric ID, provider ID, Timestamp. value) to SQL Warehouse

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFASM = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicASM).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFASM.printSchema


// COMMAND ----------

val streamingDfASM = streamingInputDFASM
.select(get_json_object(($"value").cast("string"), "$.meta").alias("meta"), ($"value").cast("string").alias("value"),$"timestamp")
.withColumnRenamed("timestamp","EnqueuedTime")
.withColumn("LoadTime",current_timestamp)


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//val asmDataSchema = spark.read.json(basePath + "asm_schema/asm.json").schema
val asmMetaSchema = spark.read.json(basePath + "asm_schema/meta.json").schema
val streamingASMMetaJson = streamingDfASM.select(from_json(col("meta"),asmMetaSchema).alias("json_header"), $"value",$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val ASMWithMetaAndDatestamp = streamingASMMetaJson
.select("json_header.Serial", "json_header.Version", "json_header.Timestamp", "value","EnqueuedTime", "LoadTime")
.withColumnRenamed("Timestamp", "timestamp")
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.filter($"timestamp" > (unix_timestamp() - lit(172800)))
//.filter($"timestamp" > lit(1593561600))

// COMMAND ----------

val ASMWithHashKeyStream = ASMWithMetaAndDatestamp.withColumn("uniqueHashKey",sha1(concat(lit("ASM"),$"Serial",$"Version", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ASMPool1")

val ASMExplodedQuery1 =  
ASMWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "24 hours")
.dropDuplicates("uniqueHashKey")
.select("Serial","Version","timestamp","Datestamp", "Hour", "value","EnqueuedTime", "LoadTime")
.writeStream
.queryName("ASMQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/ASM-kafka2bronze")
.option("path",basePath + "DeltaTable/ASM-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()

// COMMAND ----------

val streamingInputDFASMBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/ASM-bronze")

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(Json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(Seq(Json).toDS)
  //reader.json(sc.parallelize(Array(json)))
}

val schemaASM = new StructType().add("data", MapType(StringType, StringType)).add("meta", MapType(StringType, StringType))


// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ASMPool2")


val ASMExplodedQuery2 = 
 streamingInputDFASMBr
.select("value")
.writeStream
.queryName("ASMQuery2")
.trigger(Trigger.ProcessingTime("60 seconds"))
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/ASM-silver-step1")
    .foreachBatch((dataset: DataFrame, batchId: Long) => {
      dataset.persist()
      
      dataset.collect().foreach(row => {
                              //row.toSeq.foreach(col => println(col))
                              val strDf = row.toString()
                              val events = jsonToDataFrame(strDf, schemaASM)
                              events.select($"meta.Serial" as "Serial", $"meta.Version" as "Version", $"meta.Timestamp" as "timestamp", explode('data) as (Seq("Name", "Value")))
                              .withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
                              .write.format("delta").partitionBy("Datestamp").option("path",basePath + "DeltaTable/ASM-silver-step1").mode("append").save()
                              }
                             )
      
      dataset.unpersist()
      ()
    })
    .start()


// COMMAND ----------

val streamingASMSiver1 = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/ASM-silver-step1")

// COMMAND ----------

spark.conf.set("spark.sql.broadcastTimeout",  3600)

// COMMAND ----------

val metricmappingsDFASM = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings")
.filter($"metricProviderId" === 5)
.select("rawSymbol","metricProviderId","metricId")

// COMMAND ----------

val ASMJoinMetricMappingDF = streamingASMSiver1.join(
    metricmappingsDFASM,
    expr(""" 
       rawSymbol = Name
      """
    )
  )
.select("Serial","Version","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderId")
.withColumnRenamed("Serial","serialnumber")
.withColumnRenamed("timestamp","unix_timestamp")
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumn("element", lit(null).cast(StringType))
.withColumn("deviceType", lit("DEV_ANTNA"))
.withColumn("model", lit(null).cast(StringType))

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ASMPool3")

val ASMExplodedQuery3 =  
ASMJoinMetricMappingDF
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("ASMQuery3")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/ASM-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()



// COMMAND ----------

// MAGIC 
// MAGIC %md 
// MAGIC #Process Metrics - Hub Stats
// MAGIC 1. read messages from Kafka topic
// MAGIC 2. join with terminal table by rowkey
// MAGIC 3. join with router table by rowkey
// MAGIC 4. get the model serial number
// MAGIC 5. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 6. join metricmapping table by primaryRawSymbol
// MAGIC 7. join metrics table to get the Kymeta meric Id
// MAGIC 8. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to DB

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFStat = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicHubstats).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFStat.printSchema


// COMMAND ----------

//display(streamingInputDFStat)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaHubStat = StructType(
  StructField("errors", ArrayType(StringType), true) ::
    StructField("data", ArrayType(StructType(
        StructField("element", StringType, true) ::
          StructField("timestamp", ArrayType(LongType), true) ::
          StructField("mean_value", ArrayType(StringType), true) ::
          StructField("metric", StringType, true) ::
            Nil)),true) ::
    StructField("meta", StructType(    
      StructField("count", LongType, true) :: 
        Nil), true) ::
  Nil)

// COMMAND ----------

val streamingDFStat = streamingInputDFStat.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","topic","offset","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)


// COMMAND ----------

val streamingDFJsonStat = streamingDFStat.select(from_json(col("value"), schemaHubStat).alias("parsed_value"),$"topic",$"offset",$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedListStat = streamingDFJsonStat.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element", "explodedData.timestamp" , "explodedData.mean_value", "explodedData.metric","EnqueuedTime","LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricStatsExploded = explodedListStat.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"mean_value"))).select($"element",$"Exploded_TS.timestamp",$"Exploded_TS.mean_value",$"metric", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric","EnqueuedTime","LoadTime")
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

// COMMAND ----------

val HubStatsWithHashKeyStream = MetricStatsExploded.withColumn("uniqueHashKey",sha1(concat(lit("hubstats"),$"element",$"metric",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubstatsPool1")

val HubStatExplodedQuery1 =  
HubStatsWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric","EnqueuedTime","LoadTime")
.writeStream
.queryName("HubstatsQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstats-Kakfa2bronze")
.option("path",basePath + "DeltaTable/Hubstats-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

// load the stream into a dataframe for terminal table 
val factTerminal = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/terminal")
val factRouter = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/satelliterouter")
val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 1)

// COMMAND ----------

val streamingInputDFHubStatsBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Hubstats-bronze")

// COMMAND ----------

val HubStatsWithTerminal = streamingInputDFHubStatsBr.join(factTerminal).where(streamingInputDFHubStatsBr("element") === factTerminal("RowKey"))
.select("element","mean_value","timestamp","Datestamp", "metric", "coremoduleid")
.withColumnRenamed("coremoduleid", "SatRouterID")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val HubStatsWithRouterID = HubStatsWithTerminal.join(factRouter).where(HubStatsWithTerminal("SatRouterID") === factRouter("RowKey")).select("element","mean_value","unix_timestamp","Datestamp",  "metric", "SatRouterID", "serialnumber","model")
//HubStatsWithRouterID.printSchema

// COMMAND ----------

val HubStatsJoinMetricMappingRxTx = HubStatsWithRouterID.join(
    metricmappingsDF,
    expr(""" 
      metric = rawSymbol
      """
    )
  )
.select("element","unix_timestamp","Datestamp", "mean_value","metric", "serialnumber","model","metricId", "metricProviderID")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("mean_value", "value")
.withColumn("deviceType", lit("DEV_MODEM"))

//HubStatsJoinMetricMappingRxTx.printSchema

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubstatsPool2")

val HubStatsExplodedQuery2 =  
HubStatsJoinMetricMappingRxTx
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("HubstatsQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstats-silver-step1")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

// MAGIC %md 
// MAGIC #Process Metrics - Hub Status
// MAGIC 1. read messages from Kafka topic
// MAGIC 2. join with terminal table by rowkey
// MAGIC 3. join with router table by rowkey
// MAGIC 4. get the model serial number
// MAGIC 5. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 6. join metricmapping table by primaryRawSymbol
// MAGIC 7. join metrics table to get the Kymeta meric Id
// MAGIC 8. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to DB

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFStatus = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicHubstatus).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDF.printSchema


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaHubStatus = StructType(
  StructField("errors", ArrayType(StringType), true) ::
    StructField("data", ArrayType(StructType(
        StructField("element_id", StringType, true) ::
          StructField("timestamp", ArrayType(LongType), true) ::
          StructField("value", ArrayType(StringType), true) ::
          StructField("metric_id", StringType, true) ::
            Nil)),true) ::
    StructField("meta", StructType(    
      StructField("count", LongType, true) :: 
        Nil), true) ::
  Nil)

// COMMAND ----------

val streamingDFStatus=streamingInputDFStatus.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)


// COMMAND ----------


val streamingDFJsonStatus = streamingDFStatus.select(from_json(col("value"), schemaHubStatus).alias("parsed_value"),$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedListStatus = streamingDFJsonStatus.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element_id", "explodedData.timestamp" , "explodedData.value", "explodedData.metric_id","EnqueuedTime","LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricStatusExploded = explodedListStatus.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"value"))).select($"element_id",$"Exploded_TS.timestamp",$"Exploded_TS.value",$"metric_id", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

//30 days - 2592000
//14 days - 1209600

// COMMAND ----------

val HubStatusWithHashKeyStream = MetricStatusExploded.withColumn("uniqueHashKey",sha1(concat(lit("hubstatus"),$"element_id",$"metric_id",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubstatusPool1")

val HubStatusExplodedQuery1 =  
HubStatusWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("element_id","value","timestamp","Datestamp", "Hour", "metric_id","EnqueuedTime","LoadTime")
.writeStream
.queryName("HubstatusQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstatus-kafka2bronze")
.option("path",basePath + "DeltaTable/Hubstatus-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

val streamingInputDFHubStatusBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Hubstatus-bronze")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val HubStatusWithTerminal = streamingInputDFHubStatusBr.join(factTerminal).where(streamingInputDFHubStatusBr("element_id") === factTerminal("RowKey"))
.select("element_id","value","timestamp","Datestamp", "metric_id", "coremoduleid")
.withColumnRenamed("coremoduleid", "SatRouterID")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val HubStatusWithRouterID = HubStatusWithTerminal.join(factRouter).where(HubStatusWithTerminal("SatRouterID") === factRouter("RowKey")).select("element_id","value","unix_timestamp","Datestamp",  "metric_id", "SatRouterID", "serialnumber","model")
//display(HubStatusWithRouterID)

// COMMAND ----------

val HubStatusJoinMetricMappingRxTx = HubStatusWithRouterID.join(
    metricmappingsDF,
    expr(""" 
      metric_id = rawSymbol
      """
    )
  )
.select("element_id","unix_timestamp","Datestamp", "value","metric_id", "serialnumber","model","metricId", "metricProviderID")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("metric_id", "metric")
.withColumnRenamed("element_id", "element")
.withColumn("deviceType", lit("DEV_MODEM"))
//HubStatusJoinMetricMappingRxTx.printSchema
//display(HubStatusJoinMetricMappingRxTx)

// COMMAND ----------

val HubStatusAfterNormalize = HubStatusJoinMetricMappingRxTx
.withColumn("value",
       when(col("Kymeta_metricId") === 116 && col("metricProviderID") === 1, when(col("value") === "1", "1").when(col("value") === "6", "3").when((col("value") === "2" || col("value") === "3" || col("value") === "4" || col("value") === "5"), "0").otherwise("2"))
      .otherwise(col("value")))

//display(HubStatusAfterNormalize)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubstatusPool2")

val HubStatusExplodedQuery2 =  
HubStatusAfterNormalize
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("HubstatusQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstatus-silver-step1")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

// MAGIC %md 
// MAGIC #Process Metrics - Hub Usage
// MAGIC 1. read messages from Kafka topic hubusage
// MAGIC 2. join with sspc table by rowkey
// MAGIC 3. join with terminalserviceplan table by rowkey
// MAGIC 4. join terminal table by rowkey
// MAGIC 5. join router table by rowkey
// MAGIC 6. get the model serial number
// MAGIC 7. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 8. join metricmapping table by primaryRawSymbol
// MAGIC 9. join metrics table and sspc table (there will be 3 metrics for usage, USAGE_NMS, USAGE_MGMT, USAGE_DATA, so you need the sspc name and hub metric id) to get the Kymeta metric Id
// MAGIC 10. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to DB

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFHubUsage = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicHubusage).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDF.printSchema


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaHubUsage = StructType(
  StructField("errors", ArrayType(StringType), true) ::
    StructField("data", ArrayType(StructType(
        StructField("element", StringType, true) ::
          StructField("timestamp", ArrayType(LongType), true) ::
          StructField("mean_value", ArrayType(StringType), true) ::
          StructField("metric", StringType, true) ::
            Nil)),true) ::
    StructField("meta", StructType(    
      StructField("count", LongType, true) :: 
        Nil), true) ::
  Nil)

// COMMAND ----------

val streamingDFHubUsage = streamingInputDFHubUsage.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)


// COMMAND ----------

val streamingDFJsonHubUsage = streamingDFHubUsage.select(from_json(col("value"), schemaHubUsage).alias("parsed_value"),$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedListHubUsage = streamingDFJsonHubUsage.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element", "explodedData.timestamp" , "explodedData.mean_value", "explodedData.metric","EnqueuedTime", "LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricHubUsageExploded = explodedListHubUsage.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"mean_value"))).select($"element",$"Exploded_TS.timestamp",$"Exploded_TS.mean_value",$"metric", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))
//.filter($"timestamp" > lit(1593561600))

// COMMAND ----------

val HubUsageWithHashKeyStream = MetricHubUsageExploded.withColumn("uniqueHashKey",sha1(concat(lit("hubusage"),$"element",$"metric",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubusagePool1")

val HubUsageExplodedQuery1 =  
HubUsageWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric","EnqueuedTime","LoadTime")
.writeStream
.queryName("HubusageQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubusage-kafka2bronze")
.option("path",basePath + "DeltaTable/Hubusage-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

// load the stream into a dataframe for sspc table 
val factSSPC = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/sspc")
//display(factSSPC)
// load the stream into a dataframe for service plan table 
val factServicePlan = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/servicePlan")
//display(factServicePlan)

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFHubUsageBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Hubusage-bronze")
//streamingInputDFHubUsage.printSchema


// COMMAND ----------

val HubUsageWithSSPC = streamingInputDFHubUsageBr.join(factSSPC).where(streamingInputDFHubUsageBr("element") === factSSPC("RowKey_SSPC")).select("element", "timestamp" , "mean_value", "metric", "Datestamp", "PartitionKey_SSPC", "UsageName")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val MetricHubUsageWithServicePlan = HubUsageWithSSPC.join(factServicePlan).where(HubUsageWithSSPC("PartitionKey_SSPC") === factServicePlan("RowKey_ServicePlan")).select("element","unix_timestamp","mean_value","metric", "Datestamp", "UsageName", "PartitionKey_ServicePlan")

//MetricUsageWithServicePlan.printSchema

// COMMAND ----------

val HubUsageWithTerminal = MetricHubUsageWithServicePlan.join(factTerminal).where(MetricHubUsageWithServicePlan("PartitionKey_ServicePlan") === factTerminal("RowKey")).select("element","mean_value","unix_timestamp","Datestamp", "metric", "UsageName", "coremoduleid")
.withColumnRenamed("coremoduleid", "SatRouterID")


val HubUsageWithSatRouterID = HubUsageWithTerminal.join(factRouter).where(HubUsageWithTerminal("SatRouterID") === factRouter("RowKey")).select("element","mean_value","unix_timestamp","Datestamp", "metric", "UsageName", "SatRouterID", "serialnumber", "model")
//HubStatsWithRouterID.printSchema

// COMMAND ----------

val HubUsageJoinMetricMappingRxTx = HubUsageWithSatRouterID.join(
    metricmappingsDF,
    expr(""" 
      UsageName = mappingType and metric = rawSymbol 
      """
    )
  )
.select("element","unix_timestamp","Datestamp", "mean_value","metric", "serialnumber","model","metricId", "metricProviderID")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("mean_value", "value")
.withColumn("deviceType", lit("DEV_MODEM"))
//HubUsageJoinMetricMappingRxTx.printSchema

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubusagePool2")

val HubUsageExplodedQuery2 =  
HubUsageJoinMetricMappingRxTx
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("HubusageQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubusage-silver-step1")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

// MAGIC %md 
// MAGIC #Process Metrics - Intelsat Usage
// MAGIC 1. read messages from Kafka topic intelsatusage
// MAGIC 2. get the NetworkProfile->ID and join with sspc table by rowkey to get the usage Name
// MAGIC 3. get the terminal ID and join terminal table by rowkey to get sat router ID
// MAGIC 4. join router table by rowkey and sat router ID
// MAGIC 5. get the model serial number
// MAGIC 6. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 7. join metricmapping table by primaryRawSymbol and mappingType by the usage->byteReceived and byteTransmitted and usage type (USAGE_NMS, USAGE_MGMT, USAGE_DATA)
// MAGIC 8. get the Kymeta metric Id from above join from metric mapping table
// MAGIC 9. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to delta table
// MAGIC 9. stream the data (remote ID, Metric ID, provider ID, Timestamp. value) to SQL Warehouse

// COMMAND ----------


import org.apache.spark.sql.functions._

val streamingInputDFIntelsatUsage = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicIntelsatusage).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()

//streamingInputDFIntelsatUsage.printSchema


// COMMAND ----------


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaIntelsatUsage = StructType(
  StructField("errors", ArrayType(StringType), true) ::
    StructField("usages", ArrayType(StructType(
        StructField("terminalId", StringType, true) ::
        StructField("requestedStartTimestamp", LongType, true) ::
        StructField("resolutionSeconds", LongType, true) ::
            StructField("networkProfiles", ArrayType(StructType(
              StructField("id", StringType, true) ::
              StructField("usages", ArrayType(StructType(
                StructField("bytesReceived", LongType, true) ::
                StructField("bytesTransmitted", LongType, true) ::
                StructField("timestamp", LongType, true) ::
                  Nil)),true) ::
              Nil)),true) ::
        Nil)), true) ::
  Nil)
  

// COMMAND ----------


val streamingDFIntelsatUsage=streamingInputDFIntelsatUsage.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","topic","offset","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)


// COMMAND ----------


val streamingDFIntelsatUsageJson = streamingDFIntelsatUsage.select(from_json(col("value"), schemaIntelsatUsage).alias("parsed_value"),$"topic",$"offset",$"EnqueuedTime",$"LoadTime")


// COMMAND ----------


val explodedListIntelsatUsage = streamingDFIntelsatUsageJson.withColumn("explodedData",explode(col("parsed_value.usages"))).select("explodedData.terminalId", "explodedData.networkProfiles","EnqueuedTime", "LoadTime")
//display(explodedList)


// COMMAND ----------


val explodedNetworkProfileIntelsat = explodedListIntelsatUsage.withColumn("explodedNetworkProfile",explode(col("networkProfiles"))).drop("networkProfiles").select("terminalId", "explodedNetworkProfile.id" ,"explodedNetworkProfile.usages","EnqueuedTime", "LoadTime")



// COMMAND ----------


val explodedIntelsatUsages = explodedNetworkProfileIntelsat.withColumn("explodedUsages",explode(col("usages"))).drop("usages").select("terminalId", "id" ,"explodedUsages.bytesReceived","explodedUsages.bytesTransmitted","explodedUsages.timestamp","EnqueuedTime", "LoadTime").withColumnRenamed("id", "SSPCId")
//.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

//explodedUsages.printSchema


// COMMAND ----------


val dfIntelsatUsageAddup = explodedIntelsatUsages
.withWatermark("EnqueuedTime", "10 seconds")
.groupBy("terminalId", "SSPCId", "timestamp", "EnqueuedTime", "LoadTime")
.agg((sum($"bytesReceived")).alias("bytesReceived"),(sum($"bytesTransmitted")).alias("bytesTransmitted"))
//dfIntelsatUsageAddup.printSchema


// COMMAND ----------


val IntelsatUsageDatestamp = dfIntelsatUsageAddup.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))


// COMMAND ----------


//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "IntelsatUsagePool1")

val IntelsatUsageExplodedQuery1 =  
IntelsatUsageDatestamp
.select("terminalId","SSPCId","timestamp","Datestamp", "Hour", "bytesReceived","bytesTransmitted","EnqueuedTime", "LoadTime")
.writeStream
.queryName("IntelsatUsageQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/IntelsatUsage-bronze")
.option("path",basePath + "DeltaTable/IntelsatUsage-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()


// COMMAND ----------


val streamingInputDFIntelsatUsageBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/IntelsatUsage-bronze")
  

// COMMAND ----------


val IntelsatUsageWithHashKeyStream = streamingInputDFIntelsatUsageBr
.withColumn("uniqueHashKey",sha1(concat(lit("intelsatUsage011221"), $"terminalId", $"SSPCId", $"timestamp")))
.withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)


// COMMAND ----------


val IntelsatUsageDropDups =  
IntelsatUsageWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")


// COMMAND ----------


val metricmappingsDFIntelsatUsage = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 2)


// COMMAND ----------


val IntelsatUsageWithSSPCUsage = IntelsatUsageDropDups.join(factSSPC).where(IntelsatUsageDropDups("SSPCId") === factSSPC("RowKey_SSPC")).select("terminalId", "SSPCId" , "timestamp", "bytesReceived", "bytesTransmitted", "Datestamp","PartitionKey_SSPC", "UsageName")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


// COMMAND ----------


val IntelsatUsageWithTerminal = IntelsatUsageWithSSPCUsage.join(factTerminal).where(IntelsatUsageWithSSPCUsage("terminalId") === factTerminal("RowKey")).select("terminalId", "SSPCId" , "unix_timestamp", "bytesReceived", "bytesTransmitted", "Datestamp", "UsageName", "coremoduleid").withColumnRenamed("coremoduleid", "SatRouterID")


val IntelsatUsageWithRouterID = IntelsatUsageWithTerminal.join(factRouter).where(IntelsatUsageWithTerminal("SatRouterID") === factRouter("RowKey")).select("terminalId", "SSPCId" , "unix_timestamp", "bytesReceived", "bytesTransmitted", "Datestamp", "UsageName", "SatRouterID", "serialnumber","model")
//IntelsatUsageWithRouterID.printSchema


// COMMAND ----------


val IntelsatUsageJoinMetricMappingRxTx = IntelsatUsageWithRouterID.join(
    metricmappingsDFIntelsatUsage,
    expr(""" 
      UsageName = mappingType and (rawSymbol = 'bytesReceived' or rawSymbol = 'bytesTransmitted')
      """
    )
  )
.withColumn("value", when(col("rawSymbol") === "bytesReceived",col("bytesReceived").cast(StringType)).when(col("rawSymbol") === "bytesTransmitted",col("bytesTransmitted").cast(StringType)))
.withColumnRenamed("rawSymbol","metric")
.select("terminalId", "SSPCId", "unix_timestamp", "metric", "value", "Datestamp", "serialnumber", "model", "metricId", "metricProviderId")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("SSPCId", "element")
.withColumn("deviceType", lit("DEV_MODEM"))
//IntelsatUsageJoinMetricMappingRxTx.printSchema


// COMMAND ----------


//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "IntelsatUsagePool2")


val IntelsatUsageExplodedQuery2 =  
IntelsatUsageJoinMetricMappingRxTx
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("IntelsatUsageQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/IntelsatUsage-silver-step1")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

