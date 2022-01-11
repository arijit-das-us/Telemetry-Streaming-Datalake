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
  .load(basePath + "DeltaTable/EVO-bronze").filter($"timestamp" < 1600819200)
//streamingInputDFIntelsatUsage.printSchema


// COMMAND ----------

val streamingDfEvo = streamingInputDFEVO
.select($"table",$"uniqueId",$"timestamp",$"Datestamp",get_json_object($"data", "$.data.rtt").alias("rtt"),get_json_object($"data", "$.data.rx_tcp_kbyte").alias("rx_tcp_kbyte"),get_json_object($"data", "$.data.tx_tcp_kbyte").alias("tx_tcp_kbyte"),get_json_object($"data", "$.data.temperature_celcius").alias("temperature_celcius"),get_json_object($"data", "$.data.network_id").alias("network_id"),get_json_object($"data", "$.data.snr_cal").alias("snr_cal"),get_json_object($"data", "$.data.current_state").alias("current_state"))
.na.fill("")

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val NewSchema= StructType(
    StructField("data", ArrayType(StructType(
        StructField("Name", StringType, true) ::
          StructField("Value", StringType, true) ::
            Nil)),true) ::
  Nil)

// COMMAND ----------

val DfEvoJson = streamingDfEvo
.withColumn("Jsondata", from_json(concat(lit("{\"data\":[{\"Name\": \"rtt\",\"Value\":\""), $"rtt", lit("\"},{\"Name\": \"rx_tcp_kbyte\",\"Value\":\""),$"rx_tcp_kbyte", lit("\"},{\"Name\": \"tx_tcp_kbyte\",\"Value\":\""),$"tx_tcp_kbyte", lit("\"},{\"Name\": \"temperature_celcius\",\"Value\":\""),$"temperature_celcius", lit("\"},{\"Name\": \"network_id\",\"Value\":\""),$"network_id",lit("\"},{\"Name\": \"snr_cal\",\"Value\":\""),$"snr_cal", lit("\"},{\"Name\": \"current_state\",\"Value\":\""),$"current_state",lit("\"}]}")), NewSchema))


// COMMAND ----------

val DfEvoNameValueExplode = DfEvoJson
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"table" as "TableName",$"uniqueId",$"timestamp",$"Datestamp",$"Name",$"Value")
.filter($"Value" =!= "")

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val EVOExplodedQuery =  
DfEvoNameValueExplode
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-kafka-silver1")
.option("path",basePath + "DeltaTable/EVO-silver-step1")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()