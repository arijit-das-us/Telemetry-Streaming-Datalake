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
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicPeplink).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFPeplink.printSchema


// COMMAND ----------

val streamingDfPeplink = streamingInputDFPeplink
.select(get_json_object(($"value").cast("string"), "$.meta.Serial").alias("serial"), get_json_object(($"value").cast("string"), "$.meta.GroupId").alias("groupId"), get_json_object(($"value").cast("string"), "$.meta.Timestamp").alias("timestamp"), get_json_object(($"value").cast("string"), "$.data").alias("data"), get_json_object(($"value").cast("string"), "$.data.id").alias("deviceId"), get_json_object(($"value").cast("string"), "$.data.interfaces[0].msg").alias("interfacesMsg"), $"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)




// COMMAND ----------

//display(streamingDfPeplink)

// COMMAND ----------

val peplinkWithTS = streamingDfPeplink
.withColumn("timestamp", unix_timestamp(regexp_replace($"timestamp","T", " "), "yyyy-MM-dd HH:mm:ss"))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))

// COMMAND ----------

//display(peplinkWithTS.select("serial","timestamp","groupId"))

// COMMAND ----------

val PeplinkWithHKStream = peplinkWithTS.withColumn("uniqueHashKey",sha1(concat(lit("peplink"),$"serial",$"groupId", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)


// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "PeplinkPool1")

PeplinkWithHKStream
.withWatermark("TimestampYYMMSSHHMMSS", "48 hours")
.dropDuplicates("uniqueHashKey")
.select("serial","groupId","deviceId","timestamp","Datestamp","interfacesMsg","data","EnqueuedTime","LoadTime")
.writeStream
.queryName("PeplinkQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/peplink-kafka2bronze")
.option("path",basePath + "DeltaTable/Peplink-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("120 seconds")).start()



// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDfPeplinkBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Peplink-bronze")
//streamingInputDfPeplink.printSchema


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaPeplink=spark.read.json(basePath + "Peplink/sample.json").schema

// COMMAND ----------

//val outSchema=spark.read.json(basePath + "Peplink/outSample.json").schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val NewSchemaPeplink= StructType(
    StructField("data", ArrayType(StructType(
        StructField("Name", StringType, true) ::
          StructField("Value", StringType, true) ::
            Nil)),true) ::
  Nil)

// COMMAND ----------

val DfPeplinkBr = streamingInputDfPeplinkBr.select(from_json(col("data"), schemaPeplink).alias("parsed_data"),$"serial",$"deviceId",$"groupId",$"EnqueuedTime",$"LoadTime",$"timestamp",$"Datestamp")

// COMMAND ----------

val DfPeplinkBrEx = DfPeplinkBr
.withColumn("Interfaces",(col("parsed_data.interfaces")))
.withColumn("Ethernet", $"Interfaces".getItem(0))
.withColumn("cellular", when($"Interfaces.type".getItem(1) === "gobi",$"Interfaces".getItem(1)).when($"Interfaces.type".getItem(2) === "gobi",$"Interfaces".getItem(2)).otherwise($"Interfaces".getItem(3)))
//.withColumn("cellular", $"Interfaces".getItem(3))
.withColumn("cellular_sims", $"cellular.sims")
.withColumn("cellular_sims_active", when($"cellular_sims.active".getItem(0) === "true" && instr($"cellular_sims.apn".getItem(0),"kyb")===1 ,$"cellular_sims".getItem(0)).when($"cellular_sims.active".getItem(1) === "true" && instr($"cellular_sims.apn".getItem(1),"kyb")===1 ,$"cellular_sims".getItem(1)))
.withColumn("ssid_profile",  when(instr($"parsed_data.ssid_profiles.ssid".getItem(0),"u8_Internet")===1, $"parsed_data.ssid_profiles".getItem(0)).when(instr($"parsed_data.ssid_profiles.ssid".getItem(1),"u8_Internet")===1, $"parsed_data.ssid_profiles".getItem(1)))
.select(
  $"serial",
  $"deviceId",
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
val DfPeplinkBrNorm = DfPeplinkBrEx
.withColumn("device_status", when($"device_status" === "online",1).otherwise(0))
.withColumn("local_wifi_enabled", when($"local_wifi_enabled" === true,1).otherwise(0))
.withColumn("cellular_sims_status", when($"cellular_sims_status" === true,1).otherwise(0))
.withColumn("SatConnectionStatus", when($"SatConnectionStatus" === "green",1).when($"SatConnectionStatus" === "red",0).when($"SatConnectionStatus" === "gray",3).otherwise(2))
.withColumn("CellConnectionStatus", when($"CellConnectionStatus" === "green",1).when($"CellConnectionStatus" === "red",0).when($"CellConnectionStatus" === "gray",3).otherwise(2))

// COMMAND ----------

val DfPeplinkBrJson = DfPeplinkBrNorm
.withColumn("Jsondata", from_json(concat(lit("{\"data\":[{\"Name\": \"device_status\",\"Value\":\""), $"device_status", lit("\"},{\"Name\": \"client_count\",\"Value\":\""),$"client_count", lit("\"},{\"Name\": \"latitude\",\"Value\":\""),$"latitude", lit("\"},{\"Name\": \"longitude\",\"Value\":\""),$"longitude", lit("\"},{\"Name\": \"local_wifi_enabled\",\"Value\":\""),$"local_wifi_enabled",lit("\"},{\"Name\": \"SatConnectionStatus\",\"Value\":\""),$"SatConnectionStatus", lit("\"},{\"Name\": \"CellConnectionStatus\",\"Value\":\""),$"CellConnectionStatus",lit("\"},{\"Name\": \"cellular_signals_rssi\",\"Value\":\""),$"cellular_signals_rssi", lit("\"},{\"Name\": \"cellular_signals_sinr\",\"Value\":\""),$"cellular_signals_sinr", lit("\"},{\"Name\": \"cellular_signals_rsrp\",\"Value\":\""),$"cellular_signals_rsrp", lit("\"},{\"Name\": \"cellular_signals_rsrq\",\"Value\":\""),$"cellular_signals_rsrq", lit("\"},{\"Name\": \"cellular_signal_bar\",\"Value\":\""),$"cellular_signal_bar", lit("\"},{\"Name\": \"cellular_carrier_name\",\"Value\":\""),$"cellular_carrier_name", lit("\"},{\"Name\": \"cellular_sims_status\",\"Value\":\""),$"cellular_sims_status", lit("\"},{\"Name\": \"cellular_sims_apn\",\"Value\":\""),$"cellular_sims_apn", lit("\"}]}")), NewSchemaPeplink))


// COMMAND ----------

val DfPeplinkNameValueExplode = DfPeplinkBrJson
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"serial",$"deviceId",$"groupId",$"timestamp",$"Datestamp",$"Name",$"Value")
.filter($"Value" =!= "" && (substring(col("Name"),0,15) === "cellular_signal" && ($"Value" === "0" || $"Value" === "0.0")) === false)

// COMMAND ----------

val metricmappingsDFPeplink = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 6)
//display(metricmappingsDF)

// COMMAND ----------

val PeplinkJoinMetricMappingDF = DfPeplinkNameValueExplode.join(
    metricmappingsDFPeplink,
    expr(""" 
       rawSymbol = Name
      """
    )
  )
.select("serial","deviceId","groupId","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID")
.withColumnRenamed("Serial","serialnumber")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumn("element", col("deviceId").cast(StringType))
.withColumn("deviceType", lit("DEV_CLRTR"))
.withColumn("model", lit(null).cast(StringType))

// COMMAND ----------

//display(PeplinkJoinMetricMappingDF)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "PeplinkPool2")


PeplinkJoinMetricMappingDF
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("PeplinkQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Peplink-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("120 seconds")).start()

// COMMAND ----------

// MAGIC %md 
// MAGIC #Process Metrics - Cubic usage
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

val streamingInputDFCubic = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicCubic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFCubic.printSchema


// COMMAND ----------

val streamingDfCubic = streamingInputDFCubic
.select(explode(from_json(get_json_object(($"value").cast("string"), "$.data"),ArrayType(StringType))).alias("data"),$"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)
.withColumn("timestamp",get_json_object(($"data"), "$.SessionEndDateTime"))
.withColumn("timestamp", unix_timestamp(substring($"timestamp",0,19), "yyyy-MM-dd HH:mm:ss"))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
.withColumn("Hour", from_unixtime($"timestamp","HH"))
.withColumn("SessionId",get_json_object(($"data"), "$.SessionId"))
.withColumn("MCCMNC",get_json_object(($"data"), "$.MCCMNC"))
.withColumn("ICCID",get_json_object(($"data"), "$.ICCID"))
.withColumn("MSISDN",get_json_object(($"data"), "$.MSISDN"))
.withColumn("MBUsed",get_json_object(($"data"), "$.MBUsed"))
.withColumn("MBCharged",get_json_object(($"data"), "$.MBCharged"))
.withColumn("CallStatus",get_json_object(($"data"), "$.CallStatus"))
.withColumn("APNName",get_json_object(($"data"), "$.APNName"))
.withColumn("MCC",get_json_object(($"data"), "$.MCC"))
.withColumn("MNC",get_json_object(($"data"), "$.MNC"))

// COMMAND ----------

val CubicWithHKStream = streamingDfCubic.withColumn("uniqueHashKey",sha1(concat(lit("cubicUsageForMBCharge"),$"ICCID",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "CubicPool1")


CubicWithHKStream
.withWatermark("TimestampYYMMSSHHMMSS", "48 hours")
.dropDuplicates("uniqueHashKey")
.select("SessionId","MCCMNC","ICCID","MSISDN","MBUsed","MBCharged","CallStatus","APNName","MCC","MNC","timestamp","Datestamp", "Hour","EnqueuedTime", "LoadTime")
.writeStream
.queryName("CubicQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Cubic-kafka2bronze")
.option("path",basePath + "DeltaTable/Cubic-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("120 seconds")).start()

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFCubicBronze = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Cubic-bronze")
  .select($"ICCID" as "serialnumber",round($"MBCharged".cast(DoubleType)*1024*1024).cast(LongType).cast(StringType) as "MBCharged",$"MCC",$"timestamp".cast(StringType) as "unix_timestamp",$"Datestamp")
  .filter($"MBCharged" =!= "0")

// COMMAND ----------

val NewSchemaCubic= StructType(
    StructField("data", ArrayType(StructType(
        StructField("Name", StringType, true) ::
          StructField("Value", StringType, true) ::
            Nil)),true) ::
  Nil)

// COMMAND ----------

val DfCubicBrJson = streamingInputDFCubicBronze
.withColumn("Jsondata", from_json(concat(lit("{\"data\":[{\"Name\": \"MBCharged\",\"Value\":\""), $"MBCharged", lit("\"},{\"Name\": \"MCC\",\"Value\":\""),$"MCC", lit("\"}]}")), NewSchemaCubic))


// COMMAND ----------

val DfCubicNameValueExplode = DfCubicBrJson
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"serialnumber",$"unix_timestamp",$"Datestamp",$"Name",$"Value")


// COMMAND ----------

val metricmappingsDFCubic = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 4)
//display(metricmappingsDFCubic)

// COMMAND ----------

val CubicJoinMetricMappingDF = DfCubicNameValueExplode.join(
    metricmappingsDFCubic,
    expr(""" 
       rawSymbol = Name
      """
    )
  )
.select("serialnumber","unix_timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID")
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumn("element", lit(null).cast(StringType))
.withColumn("deviceType", lit("DEV_SIM"))
.withColumn("model", lit(null).cast(StringType))

// COMMAND ----------

//display(CubicJoinMetricMappingDF)
//all data with current time stamp >= 1603139720 are valid. 

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "CubicPool2")

val CubicExplodedQuery =  
CubicJoinMetricMappingDF
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("CubicQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Cubic-silver-step2")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("120 seconds")).start()

// COMMAND ----------

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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFEvoKf = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicEvo).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFEvoKf.printSchema


// COMMAND ----------

val streamingDfEvoKf = streamingInputDFEvoKf
.select(get_json_object(($"value").cast("string"), "$.header.table").alias("table"), explode(from_json(get_json_object(($"value").cast("string"), "$.data"),ArrayType(StringType))).alias("data"),$"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)
.withColumn("timestamp",get_json_object(($"data"), "$.timestamp"))
.withColumn("uniqueId",get_json_object(($"data"), "$.unique_id"))
.withColumn("msg",get_json_object(($"data"), "$.msg"))
.filter(($"table" === "event_msg" && !$"msg".startsWith("LAT-LONG")) === false)

// COMMAND ----------

val EvoWithTableAndDatestampKf = streamingDfEvoKf
.withColumn("timestamp", unix_timestamp($"timestamp", "M/d/yy h:m:s a"))
//.filter($"timestamp" > lit(1595808000))
.filter($"timestamp" > (unix_timestamp() - lit(172800)))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
.withColumn("data",concat(lit("{\"header\": {\"table\":\""), $"table", lit("\"},\"data\":"),$"data", lit("}")))

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val EVOWithHashKeyStreamKf = EvoWithTableAndDatestampKf.withColumn("uniqueHashKey",sha1(concat(lit("EVO"),$"table",$"uniqueId", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "EvoPool1")

val EVOExplodedQueryKf =  
EVOWithHashKeyStreamKf
.withWatermark("TimestampYYMMSSHHMMSS", "72 hours")
.dropDuplicates("uniqueHashKey")
.select("table","uniqueId","timestamp","Datestamp", "data", "EnqueuedTime", "LoadTime")
.writeStream
.queryName("EvoQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-bronze-kafka2bronze")
.option("path",basePath + "DeltaTable/EVO-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()



// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFEVOBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
//  .option("startingVersion", "82655")
  .load(basePath + "DeltaTable/EVO-bronze")  
//  .filter($"timestamp" >= lit(1600819200))
//streamingInputDFEVOBr.printSchema


// COMMAND ----------

val streamingDfEvoBr = streamingInputDFEVOBr
.select(
  $"table",
  $"uniqueId",
  $"timestamp",
  $"Datestamp",
  get_json_object($"data", "$.data.rtt").alias("rtt"),
  get_json_object($"data", "$.data.rx_tcp_kbyte").alias("rx_tcp_kbyte"),
  get_json_object($"data", "$.data.tx_tcp_kbyte").alias("tx_tcp_kbyte"),
  get_json_object($"data", "$.data.temperature_celcius").alias("temperature_celcius"),
  get_json_object($"data", "$.data.network_id").alias("network_id"),
  get_json_object($"data", "$.data.snr_cal").alias("snr_cal"),
  get_json_object($"data", "$.data.current_state").alias("current_state"),
  get_json_object($"data", "$.data.rx_udp_kbyte").alias("rx_udp_kbyte"),
  get_json_object($"data", "$.data.tx_udp_kbyte").alias("tx_udp_kbyte"),       
  get_json_object($"data", "$.data.rx_icmp_kbyte").alias("rx_icmp_kbyte"),
  get_json_object($"data", "$.data.tx_icmp_kbyte").alias("tx_icmp_kbyte"),  
  get_json_object($"data", "$.data.rx_igmp_kbyte").alias("rx_igmp_kbyte"),
  get_json_object($"data", "$.data.tx_igmp_kbyte").alias("tx_igmp_kbyte"),       
  get_json_object($"data", "$.data.rx_http_kbyte").alias("rx_http_kbyte"),
  get_json_object($"data", "$.data.tx_http_kbyte").alias("tx_http_kbyte"),
  get_json_object($"data", "$.data.rx_other_kbyte").alias("rx_other_kbyte"),
  get_json_object($"data", "$.data.tx_other_kbyte").alias("tx_other_kbyte"),
  get_json_object($"data", "$.data.msg").alias("msg"),
  get_json_object($"data", "$.data.power_in_dbm").alias("power_in_dbm"),
  get_json_object($"data", "$.data.rx_power").alias("rx_power")
 )
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

val DfEvoJsonBr = streamingDfEvoBr
.withColumn("Jsondata", from_json(concat(lit("{\"data\":[{\"Name\": \"rtt\",\"Value\":\""), $"rtt", 
lit("\"},{\"Name\": \"rx_tcp_kbyte\",\"Value\":\""),$"rx_tcp_kbyte", lit("\"},{\"Name\": \"tx_tcp_kbyte\",\"Value\":\""),$"tx_tcp_kbyte", 
lit("\"},{\"Name\": \"rx_udp_kbyte\",\"Value\":\""),$"rx_udp_kbyte", lit("\"},{\"Name\": \"tx_udp_kbyte\",\"Value\":\""),$"tx_udp_kbyte", 
lit("\"},{\"Name\": \"rx_icmp_kbyte\",\"Value\":\""),$"rx_icmp_kbyte", lit("\"},{\"Name\": \"tx_icmp_kbyte\",\"Value\":\""),$"tx_icmp_kbyte", 
lit("\"},{\"Name\": \"rx_igmp_kbyte\",\"Value\":\""),$"rx_igmp_kbyte", lit("\"},{\"Name\": \"tx_igmp_kbyte\",\"Value\":\""),$"tx_igmp_kbyte", 
lit("\"},{\"Name\": \"rx_http_kbyte\",\"Value\":\""),$"rx_http_kbyte", lit("\"},{\"Name\": \"tx_http_kbyte\",\"Value\":\""),$"tx_http_kbyte", 
lit("\"},{\"Name\": \"rx_other_kbyte\",\"Value\":\""),$"rx_other_kbyte", lit("\"},{\"Name\": \"tx_other_kbyte\",\"Value\":\""),$"tx_other_kbyte", 
lit("\"},{\"Name\": \"temperature_celcius\",\"Value\":\""),$"temperature_celcius", lit("\"},{\"Name\": \"network_id\",\"Value\":\""),$"network_id",lit("\"},{\"Name\": \"power_in_dbm\",\"Value\":\""),$"power_in_dbm",lit("\"},{\"Name\": \"rx_power\",\"Value\":\""),$"rx_power",lit("\"},{\"Name\": \"snr_cal\",\"Value\":\""),$"snr_cal", lit("\"},{\"Name\": \"msg\",\"Value\":\""),$"msg", lit("\"},{\"Name\": \"current_state\",\"Value\":\""),$"current_state",lit("\"}]}")), NewSchema))


// COMMAND ----------

val DfEvoNameValueExplodeBr = DfEvoJsonBr
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"table" as "TableName",$"uniqueId",$"timestamp",$"Datestamp",$"Name",$"Value")
//changes made to filter zero values for usage metrics (to do) and negetive rtt values - timestamp 1602019784
.filter($"Value" =!= "" && $"Value" =!= "0" && ($"Name" === "rtt" && $"Value".startsWith("-")) === false)

// COMMAND ----------

import java.util.regex.Pattern
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.lit

def regexp_extractAll = udf((job: String, exp: String, groupIdx: Int) => {
      println("the column value is" + job.toString())
      val pattern = Pattern.compile(exp.toString)
      val m = pattern.matcher(job.toString)
      var result = Seq[String]()
      while (m.find) {
        val temp = 
        result =result:+m.group(groupIdx)
      }
      val sExtract = result.mkString(",")
      
      if(sExtract == null || sExtract.equals("") || sExtract.length == 0){
          sExtract
      }
      else{      
        if(sExtract.takeRight(1).equals("W") || sExtract.takeRight(1).equals("S")){
          "-" + sExtract .substring(0,sExtract.length-1)
        }else{
          sExtract.substring(0,sExtract.length-1)
        }
        
      }
    })

// COMMAND ----------

val metricmappingsDFEvo = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 3)
val uniqueidmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmapping")

// COMMAND ----------

//var leptonNetworkDf = spark.read.format("delta").load(basePath + "DeltaTable/leptonNetworks")
//var transecNetworkDf = spark.read.format("delta").load(basePath + "DeltaTable/transecNetworks")

// COMMAND ----------

val streamingEVOWithSerialBr = DfEvoNameValueExplodeBr.join(uniqueidmappingsDF,$"uniqueId" === $"NetModemId")

// COMMAND ----------

val EVOJoinMetricMappingDFBr = streamingEVOWithSerialBr.join(
    metricmappingsDFEvo,
    expr(""" 
       rawSymbol = Name and TableName = mappingType
      """
    )
  )
.select("ModemSn","TableName","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID", "ModelType", "regex")
.withColumnRenamed("ModemSn","serialnumber")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumnRenamed("TableName","element")
.withColumnRenamed("ModelType","model")
.withColumn("deviceType", lit("DEV_MODEM"))
//.withColumn("model", lit(null).cast(StringType))

//display(EVOJoinMetricMappingDF)

// COMMAND ----------

val EVOAfterNormalizeBr = EVOJoinMetricMappingDFBr
.withColumn("value",
       when(col("Kymeta_metricId") === 116 && col("metricProviderID") === 3, when(col("value") === "OK", "1").when(col("value") === "ALARM", "0").when(col("value") === "OFFLINE", "0").otherwise("2"))
      .otherwise(col("value")))
.withColumn("value",
       when(col("Kymeta_metricId") === 120 && col("metricProviderID") === 3, when(col("value") === "16", "SES-15_IG").when(col("value") === "5", "Echostar 105 IG").when(col("value") === "7", "HellasSAT_ME_IG").when(col("value") === "10", "HellasSAT_EU Beam").when(col("value") === "12", "Kumsan IG").when(col("value") === "12", "Caribbean 117W").otherwise("9999"))
      .otherwise(col("value")))
.withColumn("value",
       when(col("regex").isNotNull && col("metricProviderID") === 3, regexp_extractAll($"value",col("regex"), lit(0)))
      .otherwise(col("value")))
.filter($"Value" =!= "")
//display(HubStatusAfterNormalize)

// COMMAND ----------

//var EVOAfterMappingToBeamsBr = EVOAfterNormalizeBr
//.join(leptonNetworkDf, leptonNetworkDf("NetworkID") === EVOAfterNormalizeBr("value"), "left")
//.withColumn("value", when(col("Kymeta_metricId") === 120 && col("metricProviderID") === 3, when(col("Name").isNotNull, col("Name")).otherwise(col("value"))))
//display(EVOAfterMappingToBeamsBr)

// COMMAND ----------

val EVOWithHashKeyStreamBr = EVOAfterNormalizeBr.withColumn("uniqueHashKey",sha1(concat(lit("EVOSilver2"), $"unix_timestamp", $"Kymeta_metricId", $"metricProviderId", $"serialnumber"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unix_timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "EvoPool2")

val EVOExplodedQueryBr =  
EVOWithHashKeyStreamBr
.withWatermark("TimestampYYMMSSHHMMSS", "1 hours")
.dropDuplicates("uniqueHashKey")
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("EvoQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-Bronze2Silver")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

// COMMAND ----------

// MAGIC %md 
// MAGIC #Process Metrics - Transec Metrics

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFTranKf = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicTran).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFEvoKf.printSchema


// COMMAND ----------

val streamingDfTranKf = streamingInputDFTranKf
.select(get_json_object(($"value").cast("string"), "$.header.table").alias("table"), explode(from_json(get_json_object(($"value").cast("string"), "$.data"),ArrayType(StringType))).alias("data"),$"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)
.withColumn("timestamp",get_json_object(($"data"), "$.timestamp"))
.withColumn("uniqueId",get_json_object(($"data"), "$.unique_id"))
.withColumn("msg",get_json_object(($"data"), "$.msg"))
.filter(($"table" === "event_msg" && !$"msg".startsWith("LAT-LONG")) === false)

// COMMAND ----------

val TranWithTableAndDatestampKf = streamingDfTranKf
.withColumn("timestamp", unix_timestamp($"timestamp", "M/d/yy h:m:s a"))
//.filter($"timestamp" > lit(1595808000))
.filter($"timestamp" > (unix_timestamp() - lit(172800)))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
.withColumn("data",concat(lit("{\"header\": {\"table\":\""), $"table", lit("\"},\"data\":"),$"data", lit("}")))

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val TranWithHashKeyStreamKf = TranWithTableAndDatestampKf.withColumn("uniqueHashKey",sha1(concat(lit("TRAN"),$"table",$"uniqueId", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)



// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "TransecPool1")

val TranExplodedQueryKf =  
TranWithHashKeyStreamKf
.withWatermark("TimestampYYMMSSHHMMSS", "72 hours")
.dropDuplicates("uniqueHashKey")
.select("table","uniqueId","timestamp","Datestamp", "data", "EnqueuedTime", "LoadTime")
.writeStream
.queryName("TransecQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Transec-bronze")
.option("path",basePath + "DeltaTable/Transec-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()



// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFTranBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Transec-bronze")  
//  .filter($"timestamp" >= lit(1600819200))
//streamingInputDFEVOBr.printSchema


// COMMAND ----------

val streamingDfTranBr = streamingInputDFTranBr
.select(
  $"table",
  $"uniqueId",
  $"timestamp",
  $"Datestamp",
  get_json_object($"data", "$.data.rtt").alias("rtt"),
  get_json_object($"data", "$.data.rx_tcp_kbyte").alias("rx_tcp_kbyte"),
  get_json_object($"data", "$.data.tx_tcp_kbyte").alias("tx_tcp_kbyte"),
  get_json_object($"data", "$.data.temperature_celcius").alias("temperature_celcius"),
  get_json_object($"data", "$.data.network_id").alias("network_id"),
  get_json_object($"data", "$.data.snr_cal").alias("snr_cal"),
  get_json_object($"data", "$.data.current_state").alias("current_state"),
  get_json_object($"data", "$.data.rx_udp_kbyte").alias("rx_udp_kbyte"),
  get_json_object($"data", "$.data.tx_udp_kbyte").alias("tx_udp_kbyte"),       
  get_json_object($"data", "$.data.rx_icmp_kbyte").alias("rx_icmp_kbyte"),
  get_json_object($"data", "$.data.tx_icmp_kbyte").alias("tx_icmp_kbyte"),  
  get_json_object($"data", "$.data.rx_igmp_kbyte").alias("rx_igmp_kbyte"),
  get_json_object($"data", "$.data.tx_igmp_kbyte").alias("tx_igmp_kbyte"),       
  get_json_object($"data", "$.data.rx_http_kbyte").alias("rx_http_kbyte"),
  get_json_object($"data", "$.data.tx_http_kbyte").alias("tx_http_kbyte"),
  get_json_object($"data", "$.data.rx_other_kbyte").alias("rx_other_kbyte"),
  get_json_object($"data", "$.data.tx_other_kbyte").alias("tx_other_kbyte"),
  get_json_object($"data", "$.data.msg").alias("msg"),
  get_json_object($"data", "$.data.power_in_dbm").alias("power_in_dbm"),
  get_json_object($"data", "$.data.rx_power").alias("rx_power")
 )
.na.fill("")

// COMMAND ----------

val DfTranJsonBr = streamingDfTranBr
.withColumn("Jsondata", from_json(concat(lit("{\"data\":[{\"Name\": \"rtt\",\"Value\":\""), $"rtt", 
lit("\"},{\"Name\": \"rx_tcp_kbyte\",\"Value\":\""),$"rx_tcp_kbyte", lit("\"},{\"Name\": \"tx_tcp_kbyte\",\"Value\":\""),$"tx_tcp_kbyte", 
lit("\"},{\"Name\": \"rx_udp_kbyte\",\"Value\":\""),$"rx_udp_kbyte", lit("\"},{\"Name\": \"tx_udp_kbyte\",\"Value\":\""),$"tx_udp_kbyte", 
lit("\"},{\"Name\": \"rx_icmp_kbyte\",\"Value\":\""),$"rx_icmp_kbyte", lit("\"},{\"Name\": \"tx_icmp_kbyte\",\"Value\":\""),$"tx_icmp_kbyte", 
lit("\"},{\"Name\": \"rx_igmp_kbyte\",\"Value\":\""),$"rx_igmp_kbyte", lit("\"},{\"Name\": \"tx_igmp_kbyte\",\"Value\":\""),$"tx_igmp_kbyte", 
lit("\"},{\"Name\": \"rx_http_kbyte\",\"Value\":\""),$"rx_http_kbyte", lit("\"},{\"Name\": \"tx_http_kbyte\",\"Value\":\""),$"tx_http_kbyte", 
lit("\"},{\"Name\": \"rx_other_kbyte\",\"Value\":\""),$"rx_other_kbyte", lit("\"},{\"Name\": \"tx_other_kbyte\",\"Value\":\""),$"tx_other_kbyte", 
lit("\"},{\"Name\": \"temperature_celcius\",\"Value\":\""),$"temperature_celcius", lit("\"},{\"Name\": \"network_id\",\"Value\":\""),$"network_id",lit("\"},{\"Name\": \"power_in_dbm\",\"Value\":\""),$"power_in_dbm",lit("\"},{\"Name\": \"rx_power\",\"Value\":\""),$"rx_power",lit("\"},{\"Name\": \"snr_cal\",\"Value\":\""),$"snr_cal", lit("\"},{\"Name\": \"msg\",\"Value\":\""),$"msg", lit("\"},{\"Name\": \"current_state\",\"Value\":\""),$"current_state",lit("\"}]}")), NewSchema))


// COMMAND ----------

val DfTranNameValueExplodeBr = DfTranJsonBr
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"table" as "TableName",$"uniqueId",$"timestamp",$"Datestamp",$"Name",$"Value")
//changes made to filter zero values for usage metrics (to do) and negetive rtt values - timestamp 1602019784
.filter($"Value" =!= "" && $"Value" =!= "0" && ($"Name" === "rtt" && $"Value".startsWith("-")) === false)

// COMMAND ----------

val uniqueidmappingsTranDF = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmappingtransec")

// COMMAND ----------

val streamingTranWithSerialBr = DfTranNameValueExplodeBr.join(uniqueidmappingsTranDF,$"uniqueId" === $"NetModemId")

// COMMAND ----------

val TranJoinMetricMappingDFBr = streamingTranWithSerialBr.join(
    metricmappingsDFEvo,
    expr(""" 
       rawSymbol = Name and TableName = mappingType
      """
    )
  )
.select("ModemSn","TableName","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID", "ModelType", "regex")
.withColumnRenamed("ModemSn","serialnumber")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )
.withColumnRenamed("Name","metric")
.withColumnRenamed("Value","value")
.withColumnRenamed("metricId","Kymeta_metricId")
.withColumnRenamed("TableName","element")
.withColumnRenamed("ModelType","model")
.withColumn("deviceType", lit("DEV_MODEM"))

// COMMAND ----------

val TranAfterNormalizeBr = TranJoinMetricMappingDFBr
.withColumn("value",
       when(col("Kymeta_metricId") === 116 && col("metricProviderID") === 3, when(col("value") === "OK", "1").when(col("value") === "ALARM", "0").when(col("value") === "OFFLINE", "0").otherwise("2"))
      .otherwise(col("value")))
.withColumn("value",
       when(col("Kymeta_metricId") === 120 && col("metricProviderID") === 3, when(col("value") === "3", "Transec Inroute Group").when(col("value") === "5", "Inroute Group").when(col("value") === "6", "Inroute Group").otherwise("9999"))
      .otherwise(col("value")))
.withColumn("value",
       when(col("regex").isNotNull && col("metricProviderID") === 3, regexp_extractAll($"value",col("regex"), lit(0)))
      .otherwise(col("value")))
.filter($"Value" =!= "")
//display(HubStatusAfterNormalize)

// COMMAND ----------

//var TranAfterMappingToBeamsBr = TranAfterNormalizeBr
//.join(transecNetworkDf, transecNetworkDf("NetworkID") === TranAfterNormalizeBr("value"), "left")
//.withColumn("value", when(col("Kymeta_metricId") === 120 && col("metricProviderID") === 3, when(col("Name").isNotNull, col("Name")).otherwise(col("value"))))


// COMMAND ----------

val TranWithHashKeyStreamBr = TranAfterNormalizeBr.withColumn("uniqueHashKey",sha1(concat(lit("TransecSilver2"), $"unix_timestamp", $"Kymeta_metricId", $"metricProviderId", $"serialnumber"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"unix_timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "TransecPool2")

val TranExplodedQueryBr =  
TranWithHashKeyStreamBr
.withWatermark("TimestampYYMMSSHHMMSS", "1 hours")
.dropDuplicates("uniqueHashKey")
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("TransecQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Transec-Bronze2Silver")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()

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