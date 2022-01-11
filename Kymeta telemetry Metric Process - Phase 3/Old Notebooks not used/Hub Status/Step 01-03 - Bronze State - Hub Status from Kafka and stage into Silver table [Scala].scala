// Databricks notebook source
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

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDF = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDF.printSchema


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
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

val streamingDF=streamingInputDF.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)



// COMMAND ----------


val streamingDFJson = streamingDF.select(from_json(col("value"), schema).alias("parsed_value"),$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedList = streamingDFJson.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element_id", "explodedData.timestamp" , "explodedData.value", "explodedData.metric_id","EnqueuedTime","LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricStatsExploded = explodedList.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"value"))).select($"element_id",$"Exploded_TS.timestamp",$"Exploded_TS.value",$"metric_id", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

//30 days - 2592000
//14 days - 1209600

// COMMAND ----------

val HubStatusWithHashKeyStream = MetricStatsExploded.withColumn("uniqueHashKey",sha1(concat(lit("hubstatus"),$"element_id",$"metric_id",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

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
.trigger(Trigger.ProcessingTime("10 seconds")).start()

// COMMAND ----------

// load the stream into a dataframe for terminal table 
val factTerminal = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/terminal")
//display(factTerminal)
val factRouter = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/satelliterouter")
//display(factRouter)
val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 1)
//display(metricmappingsDF)

// COMMAND ----------

val streamingInputDFHubStatusBr = 
spark.readStream
  .format("delta")
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
.trigger(Trigger.ProcessingTime("15 seconds")).start()

// COMMAND ----------

