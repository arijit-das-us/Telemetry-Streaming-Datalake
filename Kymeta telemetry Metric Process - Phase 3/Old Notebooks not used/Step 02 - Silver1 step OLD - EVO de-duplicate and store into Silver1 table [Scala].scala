// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - EVO Metrics
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

val streamingInputDFEVO = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/EVO-bronze").filter($"timestamp" > (unix_timestamp() - lit(1209600)))
//streamingInputDFIntelsatUsage.printSchema


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val EVOWithHashKeyStream = streamingInputDFEVO.withColumn("uniqueHashKey",sha1(concat(lit("EVO-test88"),$"table",$"uniqueId", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


val EVOExplodedQuery =  
EVOWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("table","uniqueId","timestamp","Datestamp", "data")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-silver-step1")
.option("path",basePath + "DeltaTable/EVO-silver-step1")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()

