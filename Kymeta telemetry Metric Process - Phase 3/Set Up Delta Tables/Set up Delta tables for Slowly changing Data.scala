// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

def checkDataFrameEquality(actualDF: DataFrame, expectedDF: DataFrame): Boolean = {
  if(actualDF.columns.size != expectedDF.columns.size || actualDF.unionAll(expectedDF).except(actualDF.intersect(expectedDF)).count() > 0 )
    false
  else 
    true
}

// COMMAND ----------

val TerminalDf = spark.read.parquet(basePath + "terminal").select($"PartitionKey",$"RowKey",$"ObjName",$"coremoduleid".cast(StringType))
val TerminalDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/terminal")

// COMMAND ----------

//println(checkDataFrameEquality(TerminalDf,TerminalDeltaDf))

// COMMAND ----------

// Write the terminal data to the delta path
if(checkDataFrameEquality(TerminalDf,TerminalDeltaDf) == false){
  TerminalDf.write
  .format("delta")
  .partitionBy("RowKey")
  .mode("overwrite")
  .save(basePath + "DeltaTable/terminal")
}

// COMMAND ----------

val ModelTypeMappingDf = spark.read.json(basePath + "lepton/ModelModelType.json")
.withColumn("explodedData",explode(col("valueMappings"))).select("explodedData.ModelType", "explodedData.Shortcode")
//display(ModelTypeMappingDf)

// COMMAND ----------

val satelliterouterDf = spark.read.parquet(basePath + "satelliterouter").select($"PartitionKey",$"RowKey",$"serialnumber".cast(StringType),$"model")
.join(ModelTypeMappingDf, $"model" === $"ModelType", "left_outer").select($"PartitionKey",$"RowKey",$"serialnumber",$"Shortcode" as "model")

//display(satelliterouterDf)

// COMMAND ----------

//val satelliterouterDf = spark.read.parquet(basePath + "satelliterouter").select($"PartitionKey",$"RowKey",$"serialnumber".cast(StringType),$"model")
//.withColumn("model",when($"model" === "X7", "idirectx7").when($"model" === "950mp", "idirect950mp").when($"model" === "CX700", "idirectx7").otherwise(null))
val satelliterouterDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/satelliterouter")
//display(satelliterouterDeltaDf)

// COMMAND ----------

//println(checkDataFrameEquality(satelliterouterDf,satelliterouterDeltaDf))

// COMMAND ----------

// Write the Satellite Router data to the delta path
if(checkDataFrameEquality(satelliterouterDf,satelliterouterDeltaDf) ==  false){
  satelliterouterDf.write
  .format("delta")
  .partitionBy("RowKey")
  .mode("overwrite")
  .save(basePath + "DeltaTable/satelliterouter")
}

// COMMAND ----------

val types = List("DEV_MODEM", "DEV_ANTNA", "DEV_CLRTR")


val deviceHistoryDf = spark.read.parquet(basePath + "ksn/remotedevicehistory.parquet")
.filter(($"Type".isin(types:_*) && $"Model".isNull) === false)
.withColumn("Model",when($"model" === "idirectcx700", "idirectx7").otherwise($"model"))
//display(deviceHistoryDf)

// COMMAND ----------

val DupDeviceHistory = deviceHistoryDf
.filter($"removedOn".isNull)
.groupBy("remoteId","Serial","Model")
.agg((count($"remoteId")).alias("count"))
.where($"count">1)
display(DupDeviceHistory)

// COMMAND ----------

val diffDf = deviceHistoryDf.select("Serial").except(DupDeviceHistory.select("Serial"))

val SerialNumber = diffDf.select("Serial").collect().map(_(0)).toList
val updatedRemoteDeviceDf = deviceHistoryDf.filter(col("Serial").isin(SerialNumber:_*) )
//display(updatedRemoteDeviceDf)

// COMMAND ----------

val remotedevicehistoryDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/remotedevicehistory")
//println(checkDataFrameEquality(updatedRemoteDeviceDf,remotedevicehistoryDeltaDf))


// COMMAND ----------

//println(checkDataFrameEquality(updatedRemoteDeviceDf,remotedevicehistoryDeltaDf))

// COMMAND ----------

// Write the Modem History data to the delta path
if(checkDataFrameEquality(updatedRemoteDeviceDf,remotedevicehistoryDeltaDf) == false){
  updatedRemoteDeviceDf
  .write
  .format("delta")
  .partitionBy("Serial")
  .mode("overwrite")
  .save(basePath + "DeltaTable/remotedevicehistory")
}

// COMMAND ----------

val remoteDf = spark.read.parquet(basePath + "ksn/remote.parquet").select("Id","Private")
val remoteDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/remotes")
//remoteDf.printSchema

// COMMAND ----------

//println(checkDataFrameEquality(remoteDf,remoteDeltaDf))

// COMMAND ----------

// Write the Modem History data to the delta path
if(checkDataFrameEquality(remoteDf,remoteDeltaDf) == false){
  remoteDf.write
  .format("delta")
  .mode("overwrite")
  .save(basePath + "DeltaTable/remotes")
}

// COMMAND ----------

//commented out to avoid the note to fail due to HTTP 502 error
import scala.io.Source
import java.net.URL

val connection = new URL(metricmappingsURL).openConnection
connection.setRequestProperty(name, value)
val metricmappings = Source.fromInputStream(connection.getInputStream).getLines.mkString("\n")

// COMMAND ----------

val metricmappingsDF = spark.read.json(Seq(metricmappings).toDS)
val metricmappingsDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/metricmappings")
//metricmappingsDF.printSchema

// COMMAND ----------

//println(checkDataFrameEquality(metricmappingsDF,metricmappingsDeltaDf))

// COMMAND ----------

if(checkDataFrameEquality(metricmappingsDF,metricmappingsDeltaDf) == false){
  metricmappingsDF.write
  .format("delta")
  .partitionBy("rawSymbol")
  .mode("overwrite")
  .save(basePath + "DeltaTable/metricmappings")
}

// COMMAND ----------


val SSPCDfRaw = spark.read.parquet(basePath + "sspc")

val SSPCDf = SSPCDfRaw.
withColumn("UsageName",
      expr("case when instr(ObjName,'MGMT-SSPP') > 0 then 'MGMT' " +
                       "when instr(ObjName, 'NMS_SSPP') > 0 then 'NMS' " +
                       "else 'DATA' end")).select("PartitionKey","RowKey","UsageName","ObjName").withColumnRenamed("PartitionKey","PartitionKey_SSPC").withColumnRenamed("RowKey","RowKey_SSPC")


//SSPCDf.printSchema

// COMMAND ----------

val SSPCDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/sspc")
//println(checkDataFrameEquality(SSPCDf,SSPCDeltaDf))

// COMMAND ----------

//println(checkDataFrameEquality(SSPCDf,SSPCDeltaDf))

// COMMAND ----------

if(checkDataFrameEquality(SSPCDf,SSPCDeltaDf) == false){
  SSPCDf.write
  .format("delta")
  .partitionBy("RowKey_SSPC")
  .mode("overwrite")
  .save(basePath + "DeltaTable/sspc")
}

// COMMAND ----------

val servicePlanDf = spark.read.parquet(basePath + "serviceplan").select("PartitionKey","RowKey").withColumnRenamed("PartitionKey","PartitionKey_ServicePlan").withColumnRenamed("RowKey","RowKey_ServicePlan")
val servicePlanDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/servicePlan")
//servicePlanDf.printSchema

// COMMAND ----------

//println(checkDataFrameEquality(servicePlanDf,servicePlanDeltaDf))

// COMMAND ----------

if(checkDataFrameEquality(servicePlanDf,servicePlanDeltaDf) == false){
  servicePlanDf.write
  .format("delta")
  .partitionBy("RowKey_ServicePlan")
  .mode("overwrite")
  .save(basePath + "DeltaTable/servicePlan")
}


// COMMAND ----------

//commenting to stop the notebook to fail
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



var tempCodes = sqlContext.createDataFrame(Seq(
        (null, 212, "SUM", 9008),
        (null, 214, "SUM", 9008),
        (null, 216, "SUM", 9008),
        (null, 218, "SUM", 9008),
        (null, 222, "SUM", 9008),
        (null, 84, "SUM", 9008),
        (null, 85, "SUM", 9008),
        (null, 213, "SUM", 9009),
        (null, 215, "SUM", 9009),
        (null, 217, "SUM", 9009),
        (null, 219, "SUM", 9009),
        (null, 223, "SUM", 9009),
        (null, 71, "SUM", 9009),
        (null, 72, "SUM", 9009)
        )).toDF("connector", "mappingIds", "mappingType","metricId")

val factCustommetricsWithTempcodes = custommetricsDF.union(tempCodes)

// COMMAND ----------

factCustommetricsWithTempcodes.write
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
val metricsnDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/metrics-aggtype")
//display(metricsDF)

// COMMAND ----------

//println(checkDataFrameEquality(metricsDF,metricsnDeltaDf))

// COMMAND ----------

if(checkDataFrameEquality(metricsDF,metricsnDeltaDf) == false){
  metricsDF.write
  .format("delta")
  .option("overwriteSchema", "true")
  .partitionBy("id")
  .mode("overwrite")
  .save(basePath + "DeltaTable/metrics-aggtype")
}

// COMMAND ----------

val evolutionLeptonDf = spark.read.parquet(basePath + "lepton/lepton.parquet").select($"NetModemId".cast(LongType),$"ModemSn")
val evolutionModemTypeDf = spark.read.parquet(basePath + "lepton/leptonModemType.parquet").select($"ID".cast(LongType), $"SN".cast(StringType),$"ModelType")
//evolutionLeptonDf.printSchema
//display(evolutionLeptonDf.orderBy("NetModemId"))
//display(evolutionLeptonDf)

// COMMAND ----------

val evolutionSnWithModelDf = evolutionLeptonDf
.join(evolutionModemTypeDf, evolutionLeptonDf("NetModemId") === evolutionModemTypeDf("ID") && evolutionLeptonDf("ModemSn") === evolutionModemTypeDf("SN"), "left_outer")

val evoWithModelDfModelType = evolutionSnWithModelDf
.join(ModelTypeMappingDf, evolutionSnWithModelDf("ModelType") === ModelTypeMappingDf("ModelType"), "left_outer")
.select($"NetModemId",$"ModemSn",$"Shortcode" as "ModelType")

//display(evoWithModelDfModelType)

// COMMAND ----------

//val evolutionSnWithModelDf = evolutionLeptonDf
//.join(evolutionModemTypeDf, evolutionLeptonDf("NetModemId") === evolutionModemTypeDf("ID") && evolutionLeptonDf("ModemSn") === evolutionModemTypeDf("SN"), "left_outer")
//.withColumn("ModelType",when($"ModelType" === "X7", "idirectx7").when($"ModelType" === "950mp", "idirect950mp").when($"ModelType" === "iQ200Board", "idirectiq200").otherwise(null))
//.select("NetModemId","ModemSn","ModelType")

// COMMAND ----------

val evolutionDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmapping").select("NetModemId","ModemSn","ModelType")
//display(evolutionDeltaDf)

// COMMAND ----------

//println(checkDataFrameEquality(evoWithModelDfModelType,evolutionDeltaDf))

// COMMAND ----------

if(checkDataFrameEquality(evoWithModelDfModelType,evolutionDeltaDf) == false){
evoWithModelDfModelType.write
  .format("delta")
  .mode("overwrite")
  .save(basePath + "DeltaTable/uniqueidmapping")
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val transecDf = spark.read.parquet(basePath + "transec/transec.parquet").select($"NetModemId".cast(LongType),$"ModemSn")
val transecModemTypeDf = spark.read.parquet(basePath + "transec/transecModemType.parquet").select($"ID".cast(LongType), $"SN".cast(StringType),$"ModelType")

// COMMAND ----------

val tranSnWithModelDf = transecDf
.join(transecModemTypeDf, transecDf("NetModemId") === transecModemTypeDf("ID") && transecDf("ModemSn") === transecModemTypeDf("SN"), "left_outer")

val transecWithModelDfModelType = tranSnWithModelDf
.join(ModelTypeMappingDf, tranSnWithModelDf("ModelType") === ModelTypeMappingDf("ModelType"), "left_outer")
.select($"NetModemId",$"ModemSn",$"Shortcode" as "ModelType")

// COMMAND ----------

val transecDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmappingtransec").select("NetModemId","ModemSn","ModelType")
//display(evolutionDeltaDf)

// COMMAND ----------

if(checkDataFrameEquality(transecWithModelDfModelType,transecDeltaDf) == false){
transecWithModelDfModelType.write
  .format("delta")
  .mode("overwrite")
  .save(basePath + "DeltaTable/uniqueidmappingtransec")
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val transecNetworksDf = spark.read.parquet(basePath + "transec/transecNetworks.parquet").select($"NetworkID",$"Name")
//display(transecNetworksDf)
//transecNetworksDf.write
//  .format("delta")
// .mode("overwrite")
//  .save(basePath + "DeltaTable/transecNetworks")

// COMMAND ----------

val transecNerworkDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/transecNetworks").select("NetworkID","Name")
display(transecNerworkDeltaDf)

// COMMAND ----------

if(checkDataFrameEquality(transecNetworksDf,transecNerworkDeltaDf) == false){
transecNetworksDf.write
  .format("delta")
  .mode("overwrite")
  .save(basePath + "DeltaTable/transecNetworks")
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val evoNetworksDf = spark.read.parquet(basePath + "lepton/leptonNetworks.parquet").select($"NetworkID",$"Name")
//display(evoNetworksDf)
//evoNetworksDf.write
//  .format("delta")
//  .mode("overwrite")
//  .save(basePath + "DeltaTable/leptonNetworks")

// COMMAND ----------

val leptonNerworkDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/leptonNetworks").select("NetworkID","Name")
display(leptonNerworkDeltaDf)

// COMMAND ----------

if(checkDataFrameEquality(evoNetworksDf,leptonNerworkDeltaDf) == false){
evoNetworksDf.write
  .format("delta")
  .mode("overwrite")
  .save(basePath + "DeltaTable/leptonNetworks")
}