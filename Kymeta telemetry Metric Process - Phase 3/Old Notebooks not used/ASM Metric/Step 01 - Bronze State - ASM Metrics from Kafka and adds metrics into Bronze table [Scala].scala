// Databricks notebook source
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

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFASM = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
streamingInputDFASM.printSchema


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
.filter($"timestamp" > lit(1593561600))

// COMMAND ----------

//display(ASMWithMetaAndDatestamp)

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val ASMExplodedQuery =  
ASMWithMetaAndDatestamp
.select("Serial","Version","timestamp","Datestamp", "Hour", "value","EnqueuedTime", "LoadTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/ASM-bronze")
.option("path",basePath + "DeltaTable/ASM-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()