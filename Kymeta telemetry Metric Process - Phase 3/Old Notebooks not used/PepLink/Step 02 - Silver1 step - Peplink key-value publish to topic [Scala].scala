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

import org.apache.spark.sql.functions._

val streamingInputDfPeplink = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/Peplink-bronze")
//streamingInputDfPeplink.printSchema


// COMMAND ----------

//display(streamingInputDfPeplink)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema=spark.read.json(basePath + "Peplink/sample.json").schema

// COMMAND ----------

//val outSchema=spark.read.json(basePath + "Peplink/outSample.json").schema
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

val DfPeplink = streamingInputDfPeplink.select(from_json(col("data"), schema).alias("parsed_data"),$"serial",$"groupId",$"EnqueuedTime",$"LoadTime",$"timestamp",$"Datestamp")

// COMMAND ----------

//display(DfPeplink)

// COMMAND ----------

val DfPeplinkEx = DfPeplink
.withColumn("Interfaces",(col("parsed_data.interfaces")))
.withColumn("Ethernet", $"Interfaces".getItem(0))
.withColumn("cellular", when($"Interfaces.type".getItem(1) === "gobi",$"Interfaces".getItem(1)).when($"Interfaces.type".getItem(2) === "gobi",$"Interfaces".getItem(2)).otherwise($"Interfaces".getItem(3)))
//.withColumn("cellular", $"Interfaces".getItem(3))
.withColumn("cellular_sims", $"cellular.sims")
.withColumn("cellular_sims_active", when($"cellular_sims.active".getItem(0) === "true" && instr($"cellular_sims.apn".getItem(0),"kyb")===1 ,$"cellular_sims".getItem(0)).when($"cellular_sims.active".getItem(1) === "true" && instr($"cellular_sims.apn".getItem(1),"kyb")===1 ,$"cellular_sims".getItem(1)))
.withColumn("ssid_profile",  when(instr($"parsed_data.ssid_profiles.ssid".getItem(0),"u8_Internet")===1, $"parsed_data.ssid_profiles".getItem(0)).when(instr($"parsed_data.ssid_profiles.ssid".getItem(1),"u8_Internet")===1, $"parsed_data.ssid_profiles".getItem(1)))
.select(
  $"serial",
  $"groupId",
  $"timestamp",
  $"Datestamp", 
  $"parsed_data.status" as "device_status", 
  $"parsed_data.client_count" as "client_count", 
  $"parsed_data.latitude" as "latitude", 
  $"parsed_data.longitude" as "longitude", 
  $"ssid_profile.enabled" as "local_wifi_enabled", 
  $"Ethernet.status_led" as "SatConnectionStatus", 
  $"cellular.status_led" as "CellConnectionStatus", 
  $"cellular.cellular_signals.rssi" as "cellular_signals_rssi", 
  $"cellular.cellular_signals.sinr" as "cellular_signals_sinr", 
  $"cellular.cellular_signals.rsrp" as "cellular_signals_rsrp", 
  $"cellular.cellular_signals.rsrq" as "cellular_signals_rsrq", 
  $"cellular.signal_bar" as "cellular_signal_bar", 
  $"cellular.carrier_name" as "cellular_carrier_name",  
  $"cellular_sims_active.active" as "cellular_sims_status", 
  $"cellular_sims_active.apn" as "cellular_sims_apn"
)
.na.fill(0)
.na.fill("")
.na.fill(false)

// COMMAND ----------

//normalize the status metric - JIRA 8691
val DfPeplinkNorm = DfPeplinkEx
.withColumn("device_status", when($"device_status" === "online",1).otherwise(0))
.withColumn("local_wifi_enabled", when($"local_wifi_enabled" === true,1).otherwise(0))
.withColumn("cellular_sims_status", when($"cellular_sims_status" === true,1).otherwise(0))
.withColumn("SatConnectionStatus", when($"SatConnectionStatus" === "green",1).when($"SatConnectionStatus" === "red",0).when($"SatConnectionStatus" === "gray",3).otherwise(2))
.withColumn("CellConnectionStatus", when($"CellConnectionStatus" === "green",1).when($"CellConnectionStatus" === "red",0).when($"CellConnectionStatus" === "gray",3).otherwise(2))

// COMMAND ----------

//DfPeplinkNorm.printSchema
//display(DfPeplinkNorm)

// COMMAND ----------

val DfPeplinkJson = DfPeplinkNorm
.withColumn("Jsondata", from_json(concat(lit("{\"data\":[{\"Name\": \"device_status\",\"Value\":\""), $"device_status", lit("\"},{\"Name\": \"client_count\",\"Value\":\""),$"client_count", lit("\"},{\"Name\": \"latitude\",\"Value\":\""),$"latitude", lit("\"},{\"Name\": \"longitude\",\"Value\":\""),$"longitude", lit("\"},{\"Name\": \"local_wifi_enabled\",\"Value\":\""),$"local_wifi_enabled",lit("\"},{\"Name\": \"SatConnectionStatus\",\"Value\":\""),$"SatConnectionStatus", lit("\"},{\"Name\": \"CellConnectionStatus\",\"Value\":\""),$"CellConnectionStatus",lit("\"},{\"Name\": \"cellular_signals_rssi\",\"Value\":\""),$"cellular_signals_rssi", lit("\"},{\"Name\": \"cellular_signals_sinr\",\"Value\":\""),$"cellular_signals_sinr", lit("\"},{\"Name\": \"cellular_signals_rsrp\",\"Value\":\""),$"cellular_signals_rsrp", lit("\"},{\"Name\": \"cellular_signals_rsrq\",\"Value\":\""),$"cellular_signals_rsrq", lit("\"},{\"Name\": \"cellular_signal_bar\",\"Value\":\""),$"cellular_signal_bar", lit("\"},{\"Name\": \"cellular_carrier_name\",\"Value\":\""),$"cellular_carrier_name", lit("\"},{\"Name\": \"cellular_sims_status\",\"Value\":\""),$"cellular_sims_status", lit("\"},{\"Name\": \"cellular_sims_apn\",\"Value\":\""),$"cellular_sims_apn", lit("\"}]}")), NewSchema))


// COMMAND ----------

//display(DfPeplinkJson)

// COMMAND ----------

val DfPeplinkNameValueExplode = DfPeplinkJson
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"serial",$"groupId",$"timestamp",$"Datestamp",$"Name",$"Value")

// COMMAND ----------

//display(DfPeplinkNameValueExplode)

// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

DfPeplinkNameValueExplode
.select("serial","groupId","timestamp","Datestamp", "Name", "Value")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/peplink-silver-step1")
.option("path",basePath + "DeltaTable/Peplink-silver-step1")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()
