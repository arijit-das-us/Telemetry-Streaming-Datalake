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
  .load(basePath + "DeltaTable/EVO-bronze").filter($"timestamp" > (unix_timestamp() - lit(172800)))
//streamingInputDFIntelsatUsage.printSchema


// COMMAND ----------

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(Json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(Seq(Json).toDS)
  //reader.json(sc.parallelize(Array(json)))
}

//val schema = new StructType().add("data", MapType(StringType, StringType)).add("meta", MapType(StringType, StringType))
val schema = new StructType().add("data", MapType(StringType, StringType)).add("header", MapType(StringType, StringType))

// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val query = streamingInputDFEVO
.select("data")
.writeStream
.trigger(Trigger.ProcessingTime("60 seconds"))
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-kafka-silver1")
    .foreachBatch((dataset, batchId) => {
      dataset.persist()
    //  dataset.write.mode(SaveMode.Overwrite).json(s"/tmp/spark/side-output/json/batch_${batchId}")
      //val strDf = dataset.collect()(0).toString()
      //val events = jsonToDataFrame(strDf, schema)
      
      dataset.collect().foreach(row => {
                              //row.toSeq.foreach(col => println(col))
                              val strDf = row.toString()
                              val events = jsonToDataFrame(strDf, schema)
                              events.select($"header.table" as "TableName", unix_timestamp($"data.timestamp", "MM/dd/yy hh:mm:ss aaa").as("timestamp"), $"data.unique_id" as "uniqueId", explode('data) as (Seq("Name", "Value")))
                              //.filter(($"TableName" === "lat_stats" && $"Name" === "rtt") || ($"TableName" === "raw_ip_stats" && $"Name".isin("rx_tcp_kbyte","tx_tcp_kbyte")  ) || ($"TableName" === "nms_remote_status" && $"Name".isin("temperature_celcius","network_id","fll_dac")  ) || ($"TableName" === "nms_ucp_info" && $"Name" === "snr_cal"  ) || ($"TableName" === "state_change_log" && $"Name" === "current_state"  ))                            
                            //  .filter($"TableName".isin("lat_stats","raw_ip_stats","nms_remote_status","nms_ucp_info","state_change_log") )
                              .filter($"Name".isin("rtt","rx_tcp_kbyte","tx_tcp_kbyte","rx_udp_kbyte","tx_udp_kbyte","rx_icmp_kbyte","tx_icmp_kbyte","rx_igmp_kbyte","tx_igmp_kbyte","rx_http_kbyte","tx_http_kbyte","rx_other_kbyte","tx_other_kbyte","temperature_celcius","network_id","snr_cal","current_state") )
                              .select(to_json(struct("TableName", "timestamp", "uniqueId")).alias("key"), to_json(struct("Name", "Value")).alias("value"))
                              //.withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
                              .write.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("topic", TargetkafkaStep1).mode("append").save() 
                              //format("delta").partitionBy("Datestamp").option("path",basePath + "DeltaTable/EVO-silver-step1").mode("append").save()
                              }
                             )
      
      dataset.unpersist()
    })
    .start()
