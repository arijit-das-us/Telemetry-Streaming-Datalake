// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

//display(streamingInputDFStat)

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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
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
// load the stream into a dataframe for terminal table 
val factTerminal = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/terminal")
val factRouter = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/satelliterouter")

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
//display(explodedListIntelsatUsage)


// COMMAND ----------


val explodedNetworkProfileIntelsat = explodedListIntelsatUsage.withColumn("explodedNetworkProfile",explode(col("networkProfiles"))).drop("networkProfiles").select("terminalId", "explodedNetworkProfile.id" ,"explodedNetworkProfile.usages","EnqueuedTime", "LoadTime")



// COMMAND ----------


val explodedIntelsatUsages = explodedNetworkProfileIntelsat.withColumn("explodedUsages",explode(col("usages"))).drop("usages").select("terminalId", "id" ,"explodedUsages.bytesReceived","explodedUsages.bytesTransmitted","explodedUsages.timestamp","EnqueuedTime", "LoadTime").withColumnRenamed("id", "SSPCId")
//.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

//explodedUsages.printSchema
//display(explodedIntelsatUsages)

// COMMAND ----------


val dfIntelsatUsageAddup = explodedIntelsatUsages
.withWatermark("EnqueuedTime", "10 seconds")
.groupBy("terminalId", "SSPCId", "timestamp", "EnqueuedTime", "LoadTime")
.agg((sum($"bytesReceived")).alias("bytesReceived"),(sum($"bytesTransmitted")).alias("bytesTransmitted"))
//dfIntelsatUsageAddup.printSchema


// COMMAND ----------


val IntelsatUsageDatestamp = dfIntelsatUsageAddup.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
//display(IntelsatUsageDatestamp)

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
.trigger(Trigger.ProcessingTime("60 seconds")).start()


// COMMAND ----------


val streamingInputDFIntelsatUsageBr = 
spark.readStream
  .format("delta")
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
.trigger(Trigger.ProcessingTime("60 seconds")).start()
