// Databricks notebook source
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

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFIntelsatUsage = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFIntelsatUsage.printSchema


// COMMAND ----------

//display(streamingInputDFIntelsatUsage)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
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

//display(streamingDFIntelsatUsage)

// COMMAND ----------


val streamingDFIntelsatUsageJson = streamingDFIntelsatUsage.select(from_json(col("value"), schema).alias("parsed_value"),$"topic",$"offset",$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

//streamingDFIntelsatUsageJson.printSchema()

// COMMAND ----------

//display(streamingDFIntelsatUsageJson)

// COMMAND ----------

val explodedList = streamingDFIntelsatUsageJson.withColumn("explodedData",explode(col("parsed_value.usages"))).select("explodedData.terminalId", "explodedData.networkProfiles","EnqueuedTime", "LoadTime")
//display(explodedList)

// COMMAND ----------

//display(explodedList)

// COMMAND ----------

val explodedNetworkProfile = explodedList.withColumn("explodedNetworkProfile",explode(col("networkProfiles"))).drop("networkProfiles").select("terminalId", "explodedNetworkProfile.id" ,"explodedNetworkProfile.usages","EnqueuedTime", "LoadTime")


// COMMAND ----------

//display(explodedNetworkProfile)

// COMMAND ----------

val explodedUsages = explodedNetworkProfile.withColumn("explodedUsages",explode(col("usages"))).drop("usages").select("terminalId", "id" ,"explodedUsages.bytesReceived","explodedUsages.bytesTransmitted","explodedUsages.timestamp","EnqueuedTime", "LoadTime").withColumnRenamed("id", "SSPCId")
.filter($"timestamp" > lit(1593561600))
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

//explodedUsages.printSchema

// COMMAND ----------

val dfIntelsatUsageAddup = explodedUsages
.withWatermark("EnqueuedTime", "10 seconds")
.groupBy("terminalId", "SSPCId", "timestamp", "EnqueuedTime", "LoadTime")
.agg((sum($"bytesReceived")).alias("bytesReceived"),(sum($"bytesTransmitted")).alias("bytesTransmitted"))
//dfIntelsatUsageAddup.printSchema

// COMMAND ----------

val IntelsatUsageDatestamp = dfIntelsatUsageAddup.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val IntelsatUsageExplodedQuery =  
IntelsatUsageDatestamp
.select("terminalId","SSPCId","timestamp","Datestamp", "Hour", "bytesReceived","bytesTransmitted","EnqueuedTime", "LoadTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/IntelsatUsage-bronze")
.option("path",basePath + "DeltaTable/IntelsatUsage-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()