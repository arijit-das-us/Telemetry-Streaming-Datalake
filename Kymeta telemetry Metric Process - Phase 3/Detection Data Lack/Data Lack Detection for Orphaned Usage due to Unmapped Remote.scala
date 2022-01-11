// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time._
 

val cal3 = Calendar.getInstance();
val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
cal3.add(Calendar.DATE, - 30);
val thirtyDaysBack = dateFormat.format(cal3.getTime)


// COMMAND ----------

val dfSilver1 = spark.sql("select Datestamp, serialnumber, model, metricProviderId from MetricSilver2 where Datestamp > '" + thirtyDaysBack + "' and metricProviderId != 5 and serialnumber not in (select Serial from RemoteDeviceHistory)")

//val dfSilverSelected = dfSilver.select( "unix_timestamp","Datestamp", "serialnumber", "Kymeta_metricId")

// COMMAND ----------

val dfSilver2 = spark.sql("select  Datestamp, serialnumber, silver.model as model, metricProviderId from MetricSilver2 silver inner join RemoteDeviceHistory remote on silver.serialnumber = remote.Serial and RemovedOn is NULL and silver.model != remote.model where Datestamp > '"+ thirtyDaysBack + "' and metricProviderId != 5")

//and Datestamp >= date_format(AddedOn,'yyyy-MM-dd') 

// COMMAND ----------

val dfexistingUnmappedSerial = spark.read
  .format("csv")
  .option("header","True")
  .load(basePath + "report/UnmappedRemoteAgg.csv")
  .select("serialnumber")
  .dropDuplicates()
  .collect().map(_(0)).toList

// COMMAND ----------

//print(dfexistingUnmappedSerial)

// COMMAND ----------

val dfSilverUnmappedRemote = dfSilver1.union(dfSilver2)

// COMMAND ----------

val dfaggregated = dfSilverUnmappedRemote.groupBy("Datestamp", "serialnumber", "model", "metricProviderId")
.count()
.orderBy("Datestamp", "metricProviderId", "serialnumber", "model")

// COMMAND ----------

val dfaggregatedNewCol = dfaggregated
.withColumn("UnmapRemoteStatus", when($"serialnumber".isin(dfexistingUnmappedSerial:_*), "existing unmapped remote").otherwise("new unmapped remote"))

// COMMAND ----------

val providermapping = spark.sql("select * from MetricProviders")
val dfaggregatedWithProviderName = dfaggregatedNewCol.join(providermapping).where(dfaggregatedNewCol("metricProviderId") === providermapping("id"))
.select($"Datestamp",$"serialnumber",$"model",$"name".alias("ProviderName"), $"count".alias("UnmapCount"), $"UnmapRemoteStatus")
                                                                                  

// COMMAND ----------

//display(dfaggregatedWithProviderName)

// COMMAND ----------

import spark.implicits._

//dbutils.fs.rm("access@kymetabigdata.blob.core.windows.net/asmfst/fstcsvdata/ASM_FST.csv", true)
dfaggregatedWithProviderName.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("wasbs://kite-prod@kymetabigdata.blob.core.windows.net/report/UnmappedRemoteAgg.csv")

// COMMAND ----------

dfaggregatedWithProviderName.write
  .format("parquet")
  .mode("overwrite")
  .save(basePath + "DeltaTable/report/UnmappedRemoteAgg")