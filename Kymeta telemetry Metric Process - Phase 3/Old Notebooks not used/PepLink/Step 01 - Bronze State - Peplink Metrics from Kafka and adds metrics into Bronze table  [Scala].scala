// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Peplink Metrics
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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFPeplink = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
streamingInputDFPeplink.printSchema


// COMMAND ----------

val streamingDfPeplink = streamingInputDFPeplink
.select(get_json_object(($"value").cast("string"), "$.meta.Serial").alias("serial"), get_json_object(($"value").cast("string"), "$.meta.GroupId").alias("groupId"), get_json_object(($"value").cast("string"), "$.meta.Timestamp").alias("timestamp"), get_json_object(($"value").cast("string"), "$.data").alias("data"), $"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)




// COMMAND ----------

//display(streamingDfPeplink)

// COMMAND ----------

val peplinkWithTS = streamingDfPeplink
.withColumn("timestamp", unix_timestamp(regexp_replace($"timestamp","T", " "), "yyyy-MM-dd HH:mm:ss"))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))

// COMMAND ----------

//display(peplinkWithTS)

// COMMAND ----------

val PeplinkWithHKStream = peplinkWithTS.withColumn("uniqueHashKey",sha1(concat(lit("peplink"),$"serial",$"groupId", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


PeplinkWithHKStream
.withWatermark("TimestampYYMMSSHHMMSS", "48 hours")
.dropDuplicates("uniqueHashKey")
.select("serial","groupId","timestamp","Datestamp", "data", "EnqueuedTime", "LoadTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/peplink-bronze")
.option("path",basePath + "DeltaTable/Peplink-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()

