// Databricks notebook source
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

val streamingInputDF = 
spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafkaBrokers).option("subscribe", SourcekafkaTopic).option("startingOffsets", "latest").option("minPartitions", "10").option("failOnDataLoss", "false").load()
//streamingInputDF.printSchema


// COMMAND ----------

//display(streamingInputDF)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
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

val streamingDF=streamingInputDF.selectExpr("CAST(key AS STRING)","CAST(value AS STRING)","topic","offset","timestamp").withColumnRenamed("timestamp","EnqueuedTime").withColumn("LoadTime",current_timestamp)



// COMMAND ----------

//display(streamingDF)

// COMMAND ----------


val streamingDFJson = streamingDF.select(from_json(col("value"), schema).alias("parsed_value"),$"topic",$"offset",$"EnqueuedTime",$"LoadTime")

// COMMAND ----------

streamingDFJson.printSchema()

// COMMAND ----------

//display(streamingDFJson)

// COMMAND ----------

val explodedList = streamingDFJson.withColumn("explodedData",explode(col("parsed_value.data"))).select("explodedData.element", "explodedData.timestamp" , "explodedData.mean_value", "explodedData.metric","EnqueuedTime","LoadTime")
//display(explodedList)

// COMMAND ----------

//display(explodedList)

// COMMAND ----------

val MetricStatsExploded = explodedList.withColumn("Exploded_TS", explode(arrays_zip($"timestamp",$"mean_value"))).select($"element",$"Exploded_TS.timestamp",$"Exploded_TS.mean_value",$"metric", $"EnqueuedTime",$"LoadTime").withColumn("Datestamp", from_unixtime($"timestamp","yyyy-MM-dd")).withColumn("Hour", from_unixtime($"timestamp","HH"))
.filter($"timestamp" > lit(1593561600))

// COMMAND ----------

//display(MetricStatsExploded)

// COMMAND ----------

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.streaming.Trigger
// Specifying a watermark limits state because dropping duplicates requires keeping state
// Drop anyindentical rows that arrive in a one hour time frame. It needs to include the watermark field.
val HubStatExplodedQuery =  
MetricStatsExploded
.select("element","mean_value","timestamp","Datestamp", "Hour", "metric","EnqueuedTime","LoadTime")
.writeStream
.format("delta")
.partitionBy("Datestamp")
.option("checkpointLocation", basePath + "DeltaTable/_checkpoint/Hubstats-bronze")
.option("path",basePath + "DeltaTable/Hubstats-bronze")                          // Specify the output path
.outputMode("append")                                                  // Append new records to the output path
.trigger(Trigger.ProcessingTime("60 seconds")).start()