// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def smallDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Boolean = {
  if (!actualDF.schema.equals(expectedDF.schema)) {
    false
  }
  if (!actualDF.collect().sameElements(expectedDF.collect())) {
    false
  }
  true
}

// COMMAND ----------

val TerminalDf = spark.read.parquet(basePath + "terminal").select($"PartitionKey",$"RowKey",$"ObjName",$"coremoduleid".cast(StringType))
val TerminalDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/terminal")

// COMMAND ----------

println(smallDataFrameEquality(TerminalDf,TerminalDeltaDf))

// COMMAND ----------

// Write the terminal data to the delta path
TerminalDf.write
  .format("delta")
  .partitionBy("RowKey")
  .mode("overwrite")
  .save(basePath + "DeltaTable/terminal")

// COMMAND ----------

val satelliterouterDf = spark.read.parquet(basePath + "satelliterouter").select($"PartitionKey",$"RowKey",$"serialnumber".cast(StringType),$"model")
.withColumn("model",when($"model" === "X7", "idirectx7").when($"model" === "950mp", "idirect950mp").when($"model" === "CX700", "idirectx7").otherwise(null))


// COMMAND ----------

//display(satelliterouterDf.filter($"model" === "idirect950mp").orderBy($"serialnumber"))

// COMMAND ----------

// Write the Satellite Router data to the delta path
satelliterouterDf.write
  .format("delta")
  .partitionBy("RowKey")
  .mode("overwrite")
  .save(basePath + "DeltaTable/satelliterouter")

// COMMAND ----------

val deviceHistoryDf = spark.read.parquet(basePath + "ksn/remotedevicehistory.parquet")
.withColumn("Model",when($"model" === "idirectcx700", "idirectx7").otherwise($"model"))
//display(deviceHistoryDf)

// COMMAND ----------

val DupDeviceHistory = deviceHistoryDf
.filter($"removedOn".isNull)
.groupBy("remoteId","Serial")
.agg((count($"remoteId")).alias("count"))
.where($"count">1)
//display(DupDeviceHistory)

// COMMAND ----------

val diffDf = deviceHistoryDf.select("Serial").except(DupDeviceHistory.select("Serial"))

val SerialNumber = diffDf.select("Serial").collect().map(_(0)).toList
val updatedRemoteDeviceDf = deviceHistoryDf.filter(col("Serial").isin(SerialNumber:_*) )
//display(updatedRemoteDeviceDf)

// COMMAND ----------

//display(updatedRemoteDeviceDf.filter($"Model" === "idirectx7"))

// COMMAND ----------

// Write the Modem History data to the delta path
updatedRemoteDeviceDf
  .write
  .format("delta")
  .partitionBy("Serial")
  .mode("overwrite")
  .save(basePath + "DeltaTable/remotedevicehistory")

// COMMAND ----------

val remoteDf = spark.read.parquet(basePath + "ksn/remote.parquet").select("Id","Private")
//remoteDf.printSchema

// COMMAND ----------

// Write the Modem History data to the delta path
remoteDf.write
  .format("delta")
  .mode("overwrite")
  .save(basePath + "DeltaTable/remotes")

// COMMAND ----------

//not changing
import scala.io.Source
import java.net.URL

val connection = new URL(metricmappingsURL).openConnection
connection.setRequestProperty(name, value)
val metricmappings = Source.fromInputStream(connection.getInputStream).getLines.mkString("\n")

// COMMAND ----------

val metricmappingsDF = spark.read.json(Seq(metricmappings).toDS)
//metricmappingsDF.printSchema

// COMMAND ----------


metricmappingsDF.write
  .format("delta")
  .partitionBy("rawSymbol")
  .mode("overwrite")
  .save(basePath + "DeltaTable/metricmappings")

// COMMAND ----------


val SSPCDfRaw = spark.read.parquet(basePath + "sspc")

val SSPCDf = SSPCDfRaw.
withColumn("UsageName",
      expr("case when instr(ObjName,'MGMT-SSPP') > 0 then 'MGMT' " +
                       "when instr(ObjName, 'NMS_SSPP') > 0 then 'NMS' " +
                       "else 'DATA' end")).select("PartitionKey","RowKey","UsageName","ObjName").withColumnRenamed("PartitionKey","PartitionKey_SSPC").withColumnRenamed("RowKey","RowKey_SSPC")


//SSPCDf.printSchema

// COMMAND ----------

SSPCDf.write
  .format("delta")
  .partitionBy("RowKey_SSPC")
  .mode("overwrite")
  .save(basePath + "DeltaTable/sspc")

// COMMAND ----------

val servicePlanDf = spark.read.parquet(basePath + "serviceplan").select("PartitionKey","RowKey").withColumnRenamed("PartitionKey","PartitionKey_ServicePlan").withColumnRenamed("RowKey","RowKey_ServicePlan")
//servicePlanDf.printSchema

// COMMAND ----------

servicePlanDf.write
  .format("delta")
  .partitionBy("RowKey_ServicePlan")
  .mode("overwrite")
  .save(basePath + "DeltaTable/servicePlan")



// COMMAND ----------

import scala.io.Source
import java.net.URL
val connection = new URL(metricprovidersURL).openConnection
connection.setRequestProperty(name, value)
val metricproviders = Source.fromInputStream(connection.getInputStream).getLines.mkString("\n")

// COMMAND ----------

val metricprovidersDF = spark.read.json(Seq(metricproviders).toDS)
metricprovidersDF.write
  .format("delta")
  .partitionBy("id")
  .mode("overwrite")
  .save(basePath + "DeltaTable/metricproviders")

// COMMAND ----------

import scala.io.Source
import java.net.URL
val connection = new URL(custommetricsURL).openConnection
connection.setRequestProperty(name, value)
val custommetrics = Source.fromInputStream(connection.getInputStream).getLines.mkString("\n")


// COMMAND ----------

val custommetricsDF = spark.read.json(Seq(custommetrics).toDS).withColumn("mappingIds", explode(split($"mappingIds", "\\,")))
//display(custommetricsDF)

// COMMAND ----------


custommetricsDF.write
  .format("delta")
  .partitionBy("metricId")
  .mode("overwrite")
  .save(basePath + "DeltaTable/custommetrics")

// COMMAND ----------

import scala.io.Source
import java.net.URL
//not changing
val connection = new URL(metricsURL).openConnection
connection.setRequestProperty(name, value)
val metrics = Source.fromInputStream(connection.getInputStream).getLines.mkString("\n")

// COMMAND ----------

val metricsDF = spark.read.json(Seq(metrics).toDS)
//display(metricsDF)

// COMMAND ----------


metricsDF.write
  .format("delta")
  .partitionBy("id")
  .mode("overwrite")
  .save(basePath + "DeltaTable/metrics-aggtype")

// COMMAND ----------

val evolutionLeptonDf = spark.read.parquet(basePath + "lepton/lepton.parquet").select($"NetModemId".cast(LongType),$"ModemSn")
val evolutionModemTypeDf = spark.read.parquet(basePath + "lepton/leptonModemType.parquet").select($"ID".cast(LongType), $"SN".cast(StringType),$"ModelType")
//evolutionLeptonDf.printSchema
//display(evolutionLeptonDf.orderBy("NetModemId"))
//display(evolutionLeptonDf)

// COMMAND ----------

val evolutionSnWithModelDf = evolutionLeptonDf
.join(evolutionModemTypeDf, evolutionLeptonDf("NetModemId") === evolutionModemTypeDf("ID") && evolutionLeptonDf("ModemSn") === evolutionModemTypeDf("SN"), "left_outer")
.withColumn("ModelType",when($"ModelType" === "X7", "idirectx7").when($"ModelType" === "950mp", "idirect950mp").when($"ModelType" === "iQ200Board", "idirectiq200").otherwise(null))
.select("NetModemId","ModemSn","ModelType")

// COMMAND ----------

evolutionSnWithModelDf.write
  .format("delta")
  .mode("overwrite")
  .save(basePath + "DeltaTable/uniqueidmapping")