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
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Cubic-bronze")
  .select($"ICCID" as "serialnumber",round($"MBCharged".cast(DoubleType)*1024*1024).cast(LongType).cast(StringType) as "MBCharged",$"MCC",$"timestamp".cast(StringType) as "unix_timestamp",$"Datestamp")
  .filter($"MBCharged" =!= "0")

// COMMAND ----------

//check to see if we have ICICID's in bronze table
//display(streamingInputDFCubicBronze)

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

spark.conf.set(
  "fs.azure.account.key.kcsrawarchiveprod.blob.core.windows.net",
  "wJgfOCP+lkU3IIyZ6U3gPZ0UxRVRbjv6M15z29EQLPTRVeV8dY78PQW1zt/UX97VsSfLk6FGT+0MyawczWMjTQ==")

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFEvoKf = 
spark.read.textFile("wasbs://evolution@kcsrawarchiveprod.blob.core.windows.net/year=2021/month=9/*/*.json")
.union(spark.read.textFile("wasbs://evolution@kcsrawarchiveprod.blob.core.windows.net/year=2021/month=10/*/*.json"))

// COMMAND ----------

//display(streamingInputDFEvoKf)

// COMMAND ----------

val streamingDfEvoKf = streamingInputDFEvoKf
.select(get_json_object(($"value").cast("string"), "$.header.table").alias("table"), explode(from_json(get_json_object(($"value").cast("string"), "$.data"),ArrayType(StringType))).alias("data"))
.withColumn("LoadTime",current_timestamp)
.withColumn("timestamp",get_json_object(($"data"), "$.timestamp"))
.withColumn("uniqueId",get_json_object(($"data"), "$.unique_id"))
.withColumn("msg",get_json_object(($"data"), "$.msg"))
.filter(($"table" === "event_msg" && !$"msg".startsWith("LAT-LONG")) === false)

// COMMAND ----------

val EvoWithTableAndDatestampKf = streamingDfEvoKf
.withColumn("timestamp", unix_timestamp($"timestamp", "M/d/yy h:m:s a"))
.filter($"timestamp" > lit(1634317495))
//.filter($"timestamp" < lit(1635178546))
.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
.withColumn("data",concat(lit("{\"header\": {\"table\":\""), $"table", lit("\"},\"data\":"),$"data", lit("}")))

// COMMAND ----------

//display(EvoWithTableAndDatestampKf.orderBy($"Datestamp"))

// COMMAND ----------


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val EVOWithDedup =  EvoWithTableAndDatestampKf.dropDuplicates("table","uniqueId", "timestamp")
 

// COMMAND ----------

 EVOWithDedup
.select("table","uniqueId","timestamp","Datestamp", "data", "LoadTime")
.write
.format("delta")
.partitionBy("Datestamp")
.option("mergeSchema", "true")
.mode("overwrite")
.save(basePath + "DeltaTable/Batch-EVO-bronze")



// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFEVOBr = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Batch-EVO-bronze")  


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
       when(col("Kymeta_metricId") === 120 && col("metricProviderID") === 3, when(col("value") === "16", "SES-15_IG").when(col("value") === "5", "Echostar 105 IG").when(col("value") === "7", "HellasSAT_ME_IG").when(col("value") === "10", "HellasSAT_EU Beam").when(col("value") === "12", "Kumsan IG").otherwise("9999"))
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

val EVOWithHashKeyStreamBr = EVOAfterNormalizeBr.dropDuplicates("unix_timestamp", "Kymeta_metricId", "metricProviderId", "serialnumber")

// COMMAND ----------

 EVOWithHashKeyStreamBr
.select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.write
.format("delta")
.partitionBy("Datestamp")
.option("mergeSchema", "true")
.mode("overwrite")
.save(basePath + "DeltaTable/Batch-silver-step2")



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



// COMMAND ----------

// MAGIC 
// MAGIC %md 
// MAGIC #Process Metrics - Hub Stats
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

val streamingInputDFStat = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicHubstats).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFStat.printSchema


// COMMAND ----------

//display(streamingInputDFStat)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaHubStat = StructType(
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

val streamingDFStat = streamingInputDFStat.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","topic","offset","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)


// COMMAND ----------

val streamingDFJsonStat = streamingDFStat.select(from_json(col("value"), schemaHubStat).alias("parsed_value"),$"topic",$"offset",$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedListStat = streamingDFJsonStat.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element", "explodedData.timestamp" , "explodedData.mean_value", "explodedData.metric","EnqueuedTime","LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricStatsExploded = explodedListStat.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"mean_value"))).select($"element",$"Exploded_TS.timestamp",$"Exploded_TS.mean_value",$"metric", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric","EnqueuedTime","LoadTime")
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

// COMMAND ----------

val HubStatsWithHashKeyStream = MetricStatsExploded.withColumn("uniqueHashKey",sha1(concat(lit("hubstats"),$"element",$"metric",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubstatsPool1")

val HubStatExplodedQuery1 =  
HubStatsWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric","EnqueuedTime","LoadTime")
.writeStream
.queryName("HubstatsQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstats-Kakfa2bronze")
.option("path",basePath + "DeltaTable/Hubstats-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

// load the stream into a dataframe for terminal table 
val factTerminal = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/terminal")
val factRouter = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/satelliterouter")
val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 1)

// COMMAND ----------

val streamingInputDFHubStatsBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Hubstats-bronze")

// COMMAND ----------

val HubStatsWithTerminal = streamingInputDFHubStatsBr.join(factTerminal).where(streamingInputDFHubStatsBr("element") === factTerminal("RowKey"))
.select("element","mean_value","timestamp","Datestamp", "metric", "coremoduleid")
.withColumnRenamed("coremoduleid", "SatRouterID")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val HubStatsWithRouterID = HubStatsWithTerminal.join(factRouter).where(HubStatsWithTerminal("SatRouterID") === factRouter("RowKey")).select("element","mean_value","unix_timestamp","Datestamp",  "metric", "SatRouterID", "serialnumber","model")
//HubStatsWithRouterID.printSchema

// COMMAND ----------

val HubStatsJoinMetricMappingRxTx = HubStatsWithRouterID.join(
    metricmappingsDF,
    expr(""" 
      metric = rawSymbol
      """
    )
  )
.select("element","unix_timestamp","Datestamp", "mean_value","metric", "serialnumber","model","metricId", "metricProviderID")
.withColumnRenamed("metricId", "Kymeta_metricId")
.withColumnRenamed("mean_value", "value")
.withColumn("deviceType", lit("DEV_MODEM"))

//HubStatsJoinMetricMappingRxTx.printSchema

// COMMAND ----------

//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "HubstatsPool2")

val HubStatsExplodedQuery2 =  
HubStatsJoinMetricMappingRxTx
.select("element","unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
.writeStream
.queryName("HubstatsQuery2")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstats-silver-step1")
.option("path",basePath + "DeltaTable/Metric-silver-step2")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

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

import org.apache.spark.sql.functions._

val streamingInputDFStatus = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicHubstatus).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDF.printSchema


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaHubStatus = StructType(
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

val streamingDFStatus=streamingInputDFStatus.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)


// COMMAND ----------


val streamingDFJsonStatus = streamingDFStatus.select(from_json(col("value"), schemaHubStatus).alias("parsed_value"),$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedListStatus = streamingDFJsonStatus.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element_id", "explodedData.timestamp" , "explodedData.value", "explodedData.metric_id","EnqueuedTime","LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricStatusExploded = explodedListStatus.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"value"))).select($"element_id",$"Exploded_TS.timestamp",$"Exploded_TS.value",$"metric_id", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

//30 days - 2592000
//14 days - 1209600

// COMMAND ----------

val HubStatusWithHashKeyStream = MetricStatusExploded.withColumn("uniqueHashKey",sha1(concat(lit("hubstatus"),$"element_id",$"metric_id",$"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

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
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

val streamingInputDFHubStatusBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
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
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

// COMMAND ----------

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

import org.apache.spark.sql.functions._

val streamingInputDFHubUsage = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopicHubusage).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDF.printSchema


// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schemaHubUsage = StructType(
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

val streamingDFHubUsage = streamingInputDFHubUsage.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)


// COMMAND ----------

val streamingDFJsonHubUsage = streamingDFHubUsage.select(from_json(col("value"), schemaHubUsage).alias("parsed_value"),$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

val explodedListHubUsage = streamingDFJsonHubUsage.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element", "explodedData.timestamp" , "explodedData.mean_value", "explodedData.metric","EnqueuedTime", "LoadTime")
//display(explodedList)

// COMMAND ----------

val MetricHubUsageExploded = explodedListHubUsage.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"mean_value"))).select($"element",$"Exploded_TS.timestamp",$"Exploded_TS.mean_value",$"metric", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
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
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

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
  .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Hubusage-bronze")
//streamingInputDFHubUsage.printSchema


// COMMAND ----------

val HubUsageWithSSPC = streamingInputDFHubUsageBr.join(factSSPC).where(streamingInputDFHubUsageBr("element") === factSSPC("RowKey_SSPC")).select("element", "timestamp" , "mean_value", "metric", "Datestamp", "PartitionKey_SSPC", "UsageName")
.withColumn("unix_timestamp", col("timestamp").cast(StringType) )


val MetricHubUsageWithServicePlan = HubUsageWithSSPC.join(factServicePlan).where(HubUsageWithSSPC("PartitionKey_SSPC") === factServicePlan("RowKey_ServicePlan")).select("element","unix_timestamp","mean_value","metric", "Datestamp", "UsageName", "PartitionKey_ServicePlan")

//MetricUsageWithServicePlan.printSchema

// COMMAND ----------

val HubUsageWithTerminal = MetricHubUsageWithServicePlan.join(factTerminal).where(MetricHubUsageWithServicePlan("PartitionKey_ServicePlan") === factTerminal("RowKey")).select("element","mean_value","unix_timestamp","Datestamp", "metric", "UsageName", "coremoduleid")
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
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

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
//display(explodedList)


// COMMAND ----------


val explodedNetworkProfileIntelsat = explodedListIntelsatUsage.withColumn("explodedNetworkProfile",explode(col("networkProfiles"))).drop("networkProfiles").select("terminalId", "explodedNetworkProfile.id" ,"explodedNetworkProfile.usages","EnqueuedTime", "LoadTime")



// COMMAND ----------


val explodedIntelsatUsages = explodedNetworkProfileIntelsat.withColumn("explodedUsages",explode(col("usages"))).drop("usages").select("terminalId", "id" ,"explodedUsages.bytesReceived","explodedUsages.bytesTransmitted","explodedUsages.timestamp","EnqueuedTime", "LoadTime").withColumnRenamed("id", "SSPCId")
//.filter($"timestamp" > (unix_timestamp() - lit(1209600)))

//explodedUsages.printSchema


// COMMAND ----------


val dfIntelsatUsageAddup = explodedIntelsatUsages
.withWatermark("EnqueuedTime", "10 seconds")
.groupBy("terminalId", "SSPCId", "timestamp", "EnqueuedTime", "LoadTime")
.agg((sum($"bytesReceived")).alias("bytesReceived"),(sum($"bytesTransmitted")).alias("bytesTransmitted"))
//dfIntelsatUsageAddup.printSchema


// COMMAND ----------


val IntelsatUsageDatestamp = dfIntelsatUsageAddup.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))


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
.trigger(Trigger.ProcessingTime("3600 seconds")).start()


// COMMAND ----------


val streamingInputDFIntelsatUsageBr = 
spark.readStream
  .format("delta")
  .option("ignoreChanges", "true")
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
.trigger(Trigger.ProcessingTime("3600 seconds")).start()

