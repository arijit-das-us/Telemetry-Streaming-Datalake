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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingInputDFEvoKf = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDFEvoKf.printSchema


// COMMAND ----------

val streamingDfEvoKf = streamingInputDFEvoKf
.select(get_json_object(($"value").cast("string"), "$.header.table").alias("table"), explode(from_json(get_json_object(($"value").cast("string"), "$.data"),ArrayType(StringType))).alias("data"),$"timestamp".alias("EnqueuedTime"))
.withColumn("LoadTime",current_timestamp)
.withColumn("timestamp",get_json_object(($"data"), "$.timestamp"))
.withColumn("uniqueId",get_json_object(($"data"), "$.unique_id"))


// COMMAND ----------

val EvoWithTableAndDatestampKf = streamingDfEvoKf
.withColumn("timestamp", unix_timestamp($"timestamp", "M/d/yy h:m:s a"))
//.filter($"timestamp" > lit(1595808000))
//.filter($"timestamp" > (unix_timestamp() - lit(172800)))
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
.withWatermark("TimestampYYMMSSHHMMSS", "168 hours")
.dropDuplicates("uniqueHashKey")
.select("table","uniqueId","timestamp","Datestamp", "data", "EnqueuedTime", "LoadTime")
.writeStream
.queryName("EvoQuery1")
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-bronze")
.option("path",basePath + "DeltaTable/EVO-bronze")                            // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("30 seconds")).start()



// COMMAND ----------

import org.apache.spark.sql.functions._

val streamingInputDFEVOBr = 
spark.readStream
  .format("delta")
  .option("startingVersion", "82655")
  .load(basePath + "DeltaTable/EVO-bronze")  
//  .filter($"timestamp" >= lit(1600819200))
//streamingInputDFEVOBr.printSchema


// COMMAND ----------

val streamingDfEvoBr = streamingInputDFEVOBr
.select($"table",$"uniqueId",$"timestamp",$"Datestamp",get_json_object($"data", "$.data.rtt").alias("rtt"),get_json_object($"data", "$.data.rx_tcp_kbyte").alias("rx_tcp_kbyte"),get_json_object($"data", "$.data.tx_tcp_kbyte").alias("tx_tcp_kbyte"),get_json_object($"data", "$.data.temperature_celcius").alias("temperature_celcius"),get_json_object($"data", "$.data.network_id").alias("network_id"),get_json_object($"data", "$.data.snr_cal").alias("snr_cal"),get_json_object($"data", "$.data.current_state").alias("current_state"),
 get_json_object($"data", "$.data.rx_udp_kbyte").alias("rx_udp_kbyte"),
 get_json_object($"data", "$.data.tx_udp_kbyte").alias("tx_udp_kbyte"),       
 get_json_object($"data", "$.data.rx_icmp_kbyte").alias("rx_icmp_kbyte"),
 get_json_object($"data", "$.data.tx_icmp_kbyte").alias("tx_icmp_kbyte"),  
 get_json_object($"data", "$.data.rx_igmp_kbyte").alias("rx_igmp_kbyte"),
 get_json_object($"data", "$.data.tx_igmp_kbyte").alias("tx_igmp_kbyte"),       
 get_json_object($"data", "$.data.rx_http_kbyte").alias("rx_http_kbyte"),
 get_json_object($"data", "$.data.tx_http_kbyte").alias("tx_http_kbyte"),
 get_json_object($"data", "$.data.rx_other_kbyte").alias("rx_other_kbyte"),
 get_json_object($"data", "$.data.tx_other_kbyte").alias("tx_other_kbyte")
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
lit("\"},{\"Name\": \"temperature_celcius\",\"Value\":\""),$"temperature_celcius", lit("\"},{\"Name\": \"network_id\",\"Value\":\""),$"network_id",lit("\"},{\"Name\": \"snr_cal\",\"Value\":\""),$"snr_cal", lit("\"},{\"Name\": \"current_state\",\"Value\":\""),$"current_state",lit("\"}]}")), NewSchema))


// COMMAND ----------

val DfEvoNameValueExplodeBr = DfEvoJsonBr
.withColumn("explode_data", explode($"Jsondata.data"))
.withColumn("Name", $"explode_data.Name")
.withColumn("Value", $"explode_data.Value")
.select($"table" as "TableName",$"uniqueId",$"timestamp",$"Datestamp",$"Name",$"Value")
//changes made to filter zero values for usage metrics (to do) and negetive rtt values - timestamp 1602019784
.filter($"Value" =!= "" && $"Value" =!= "0" && ($"Name" === "rtt" && $"Value".startsWith("-")) === false)

// COMMAND ----------

val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings").filter($"metricProviderId" === 3)
val uniqueidmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmapping")


// COMMAND ----------

val streamingEVOWithSerialBr = DfEvoNameValueExplodeBr.join(uniqueidmappingsDF,$"uniqueId" === $"NetModemId")

// COMMAND ----------

val EVOJoinMetricMappingDFBr = streamingEVOWithSerialBr.join(
    metricmappingsDF,
    expr(""" 
       rawSymbol = Name and TableName = mappingType
      """
    )
  )
.select("ModemSn","TableName","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID", "ModelType")
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

//display(HubStatusAfterNormalize)

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