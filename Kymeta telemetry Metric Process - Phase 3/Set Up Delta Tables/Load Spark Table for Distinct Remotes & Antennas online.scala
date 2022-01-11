// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

spark.sql("set spark.databricks.delta.autoCompact.enabled = true")
spark.sql("set spark.databricks.delta.optimizeWrite.enabled = true")

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat

val cal1 = Calendar.getInstance
def currentTime = new java.util.Date();
val YearFormat = new SimpleDateFormat("yyyy")
val MonthFormat = new SimpleDateFormat("MM")

cal1.setTime(currentTime);
cal1.add(Calendar.DATE, -1)
val yesterDayYYYY = YearFormat.format(cal1.getTime)
val yesterDayMM = MonthFormat.format(cal1.getTime)


// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFGold = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("Year", substring($"dateStamp",0,4))
  .withColumn("Month", substring($"dateStamp",6,2))
 // .filter($"metricProviderId" === 5 && $"Year" === yesterDayYYYY && $"Month" >= yesterDayMM)
  .filter($"metricProviderId" === 5)
  .select("Year", "Month", "remoteId")
  .dropDuplicates()

// COMMAND ----------

//display(InputDFGold)

// COMMAND ----------

val InputDFGoldRemote = InputDFGold.filter($"remoteId".contains("-") )
val InputDFGoldAntenna = InputDFGold.filter($"remoteId".contains("-") === false )

// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDFRemote = InputDFGoldRemote.groupBy("Year", "Month")
.count().alias("Count")
.withColumn("Type", lit("RemoteOnline"))
val aggregatesDFAntenna = InputDFGoldAntenna.groupBy("Year", "Month")
.count().alias("Count")
.withColumn("Type", lit("AntennaOnline"))


// COMMAND ----------

val AggregatedDF = aggregatesDFRemote
.union(aggregatesDFAntenna)

// COMMAND ----------

AggregatedDF.write
  .format("delta")
  .partitionBy("Year","Month")
  .mode("overwrite")
  .save(basePath + "DeltaTable/Dashboard/ASMOnline")