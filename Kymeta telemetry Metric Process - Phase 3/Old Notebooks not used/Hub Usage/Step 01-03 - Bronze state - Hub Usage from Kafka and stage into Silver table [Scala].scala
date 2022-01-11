// Databricks notebook source
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

val streamingDF=streamingInputDF.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)



// COMMAND ----------


val streamingDFJson = streamingDF.select(from_json(col("value"), schema).alias("parsed_value"),$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedList = streamingDFJson.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element", "explodedData.timestamp" , "explodedData.mean_value", "explodedData.metric","EnqueuedTime", "LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricHubUsageExploded = explodedList.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"mean_value"))).select($"element",$"Exploded_TS.timestamp",$"Exploded_TS.mean_value",$"metric", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
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
.trigger(Trigger.ProcessingTime("60 seconds")).start()

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
  .load(basePath + "DeltaTable/Hubusage-bronze")
//streamingInputDFHubUsage.printSchema


// COMMAND ----------

val HubUsageWithSSPC = streamingInputDFHubUsageBr.join(factSSPC).where(streamingInputDFHubUsageBr("element") === factSSPC("RowKey_SSPC")).select("element", "timestamp" , "mean_value", "metric", "Datestamp", "PartitionKey_SSPC", "UsageName")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val MetricUsageWithServicePlan = HubUsageWithSSPC.join(factServicePlan).where(HubUsageWithSSPC("PartitionKey_SSPC") === factServicePlan("RowKey_ServicePlan")).select("element","unix_timestamp","mean_value","metric", "Datestamp", "UsageName", "PartitionKey_ServicePlan")

//MetricUsageWithServicePlan.printSchema

// COMMAND ----------

val HubUsageWithTerminal = MetricUsageWithServicePlan.join(factTerminal).where(MetricUsageWithServicePlan("PartitionKey_ServicePlan") === factTerminal("RowKey")).select("element","mean_value","unix_timestamp","Datestamp", "metric", "UsageName", "coremoduleid")
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
.trigger(Trigger.ProcessingTime("60 seconds")).start()