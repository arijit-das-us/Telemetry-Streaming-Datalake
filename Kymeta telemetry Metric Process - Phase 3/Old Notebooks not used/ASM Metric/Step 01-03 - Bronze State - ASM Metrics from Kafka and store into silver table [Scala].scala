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
.filter($"timestamp" > (unix_timestamp() - lit(1209600)))
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

val schema = new StructType().add("data", MapType(StringType, StringType)).add("meta", MapType(StringType, StringType))


// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ASMPool2")


val query = 
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
                              val events = jsonToDataFrame(strDf, schema)
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
  .load(basePath + "DeltaTable/ASM-silver-step1")

// COMMAND ----------

spark.conf.set("spark.sql.broadcastTimeout",  3600)

// COMMAND ----------

val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings")
.filter($"metricProviderId" === 5)
.select("rawSymbol","metricProviderId","metricId")

// COMMAND ----------

//display(metricmappingsDF)

// COMMAND ----------

val ASMJoinMetricMappingDF = streamingASMSiver1.join(
    metricmappingsDF,
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

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ASMPool3")

val ASMExplodedQuery =  
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

/*
import org.apache.spark.sql.{ForeachWriter, Row}
//import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ASMPool2")

val query2 = 
streamingInputDFASMBr
.select("value")
.writeStream
.queryName("ASMQuery2")
.trigger(Trigger.ProcessingTime("60 seconds"))
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/ASM-silver-step1")
    .foreachBatch((dataset, batchId) => {
      dataset.persist()
      
      dataset.collect().foreach(row => {
                              //row.toSeq.foreach(col => println(col))
                              val strDf = row.toString()
                              val events = jsonToDataFrame(strDf, schema)
                              events.select($"meta.Serial" as "Serial", $"meta.Version" as "Version", $"meta.Timestamp" as "timestamp", explode('data) as (Seq("Name", "Value")))
                              .withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd"))
                              .join(
                                    metricmappingsDF,
                                    expr(""" 
                                         rawSymbol = Name
                                          """
                                          )
                                    )
                                .select("Serial","Version","timestamp","Datestamp", "Name", "Value", "metricId", "metricProviderID")
                                .withColumnRenamed("Serial","serialnumber")
                                .withColumnRenamed("timestamp","unix_timestamp")
                                .withColumnRenamed("Name","metric")
                                .withColumnRenamed("Value","value")
                                .withColumnRenamed("metricId","Kymeta_metricId")
                                .withColumn("element", lit(null).cast(StringType))
                                .withColumn("deviceType", lit("DEV_ANTNA"))
                                .withColumn("model", lit(null).cast(StringType))
                                .select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
                                .write.format("delta").partitionBy("Datestamp").option("path",basePath + "DeltaTable/Metric-silver-step2").mode("append").save()
                              }
                             )
      
      dataset.unpersist()
    })
    .start()
    */
