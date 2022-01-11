// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - EVO Metric
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

val metricmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings")
//metricmappingsDF.printSchema
//display(metricmappingsDF.filter($"metricProviderId" === 3))

// COMMAND ----------

val uniqueidmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmapping")
//display(uniqueidmappingsDF)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val streamingEVOSiver1 = 
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/EVO-bronze")
//streamingInputDFIntelsatUsage.printSchema
//display(streamingEVOSiver1)

// COMMAND ----------

import org.apache.spark.sql.functions.{col, udf}
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

val query = streamingEVOSiver1
.select("data")
.writeStream
.trigger(Trigger.ProcessingTime("30 seconds"))
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/EVO-silver-step2")
    .foreachBatch((dataset, batchId) => {
      dataset.persist()      
      dataset.collect().foreach(row => {
       //row.toSeq.foreach(col => println(col))
       val strDf = row.toString()
       val events = jsonToDataFrame(strDf, schema)
       events
         .select($"header.table" as "TableName", unix_timestamp($"data.timestamp", "MM/dd/yy hh:mm:ss aaa").as("timestamp"), $"data.unique_id" as "uniqueId", explode('data) as (Seq("Name", "Value")))
         .join(metricmappingsDF,expr("""rawSymbol = Name and TableName = mappingType""" ))
         .join(uniqueidmappingsDF,$"uniqueId" === $"NetModemId")
         .select($"ModemSn" as "serialnumber",$"TableName" as "element",$"timestamp".cast(StringType) as "unix_timestamp", $"Name" as "metric", $"Value" as "value", $"metricId" as "Kymeta_metricId", $"metricProviderID", from_unixtime($"timestamp","yyyy-MM-dd").as("Datestamp"))
         .withColumn("deviceType", lit("DEV_MODEM"))
         .withColumn("model", lit(null).cast(StringType))
         .withColumn("value", when(col("Kymeta_metricId") === 116 && col("metricProviderID") === 3, when(col("value") === "OK", "1").when(col("value") === "ALARM", "0").when(col("value") === "OFFLINE", "0").otherwise("2"))
         .otherwise(col("value")))
         .select("element", "unix_timestamp", "Datestamp", "metric", "value", "serialnumber", "model", "deviceType", "Kymeta_metricId", "metricProviderId")
         .write.format("delta").partitionBy("Datestamp").option("path",basePath + "DeltaTable/Metric-silver-step2").mode("append").save()
                }
         )
      
      dataset.unpersist()
    })
    .start()
