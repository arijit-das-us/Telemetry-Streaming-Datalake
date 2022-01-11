// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - ASM Metric
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
spark.readStream
  .format("delta")
  .load(basePath + "DeltaTable/ASM-bronze").filter($"timestamp" > (unix_timestamp() - lit(1209600)))
//streamingInputDFIntelsatUsage.printSchema
//display(streamingInputDFASM)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
//filter records with older date less than two weeks

val ASMWithHashKeyStream = streamingInputDFASM.withColumn("uniqueHashKey",sha1(concat(lit("ASM"),$"Serial",$"Version", $"timestamp"))).withColumn("TimestampYYMMSSHHMMSS", from_unixtime($"timestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)

//IntelsatUsageWithHashKeyStream.printSchema


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

val schema = new StructType().add("data", MapType(StringType, StringType)).add("meta", MapType(StringType, StringType))


// COMMAND ----------

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val query = 
ASMWithHashKeyStream
.withWatermark("TimestampYYMMSSHHMMSS", "24 hours")
.dropDuplicates("uniqueHashKey")
.select("value")
.writeStream
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
                              .write.format("delta").partitionBy("Datestamp").option("path",basePath + "DeltaTable/ASM-silver-step1").mode("append").save()
                              }
                             )
      
      dataset.unpersist()
    })
    .start()
