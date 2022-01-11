// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from HubstatsBronze 
// MAGIC where timestamp >= 1600895700 and timestamp <= 1600896300
// MAGIC --order by timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct metric from HubstatsBronze where timestamp >= 1600895700 and timestamp <= 1600896300

// COMMAND ----------

// MAGIC %sql
// MAGIC select element, unix_timestamp, Datestamp, metric, value, serialnumber, model, deviceType, Kymeta_metricId, metricProviderId from MetricSilver2 where Datestamp like "2021-01-%" and unix_timestamp >= 1600905600 --and unix_timestamp <= 1600896300
// MAGIC and metricProviderId = 2 and element = 1950773
// MAGIC order by unix_timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select element, unix_timestamp, Datestamp, metric, value, serialnumber, model, deviceType, Kymeta_metricId, metricProviderId from MetricSilver2 where Datestamp = "2020-09-24" and unix_timestamp >= 1600905600 --and unix_timestamp <= 1600896300
// MAGIC and metricProviderId = 2
// MAGIC order by unix_timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select element, unix_timestamp, Datestamp, metric, value, serialnumber, model, deviceType, Kymeta_metricId, metricProviderId from MetricSilver2 where Datestamp = "2020-11-28" --and unix_timestamp >= 1600905600 --and unix_timestamp <= 1600896300
// MAGIC and 
// MAGIC metricProviderId = 2 and serialnumber = '69009'
// MAGIC order by unix_timestamp desc

// COMMAND ----------

of t%sql
select * from MetricGold where metricProviderId = 2 --and kymetaMetricId = 9003 -- and unixTimestamp > 1598918400
--select * from MetricGold where metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and unixTimestamp > 1598918400
and remoteId = 'a739283e-0cc6-4126-ad30-e922882f2a8d' and kymetaMetricId = 9003
order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from IntelsatusageBronze where Datestamp = "2020-11-28" 
// MAGIC --timestamp = 1604586900 --and unix_timestamp <= 1600896300
// MAGIC  and SSPCId = 1897693 and terminalId = 1897682
// MAGIC order by timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 2 and kymetaMetricId = 9003 -- and unixTimestamp > 1598918400
// MAGIC --select * from MetricGold where metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and unixTimestamp > 1598918400
// MAGIC order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricSilver2 where metricProviderId = 2 and model = 'CX780'

// COMMAND ----------

// MAGIC %sql
// MAGIC select element, unix_timestamp, Datestamp, metric, value, serialnumber, model, deviceType, Kymeta_metricId, metricProviderId from MetricSilver2 where Datestamp = "2020-09-24" --and unix_timestamp >= 1600905600 --and unix_timestamp <= 1600896300
// MAGIC and metricProviderId = 5
// MAGIC order by unix_timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from AsmBronze where Datestamp = "2020-09-24" --and timestamp = 1597753500 --and unix_timestamp <= 1600896300
// MAGIC order by timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 3 and kymetaMetricId = 9003 and Datestamp = "2020-09-25"  
// MAGIC --order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 2 and elementId = 1950773 and Datestamp = "2020-01-02" 

// COMMAND ----------

// MAGIC %sql
// MAGIC select max(unixTimestamp) from MetricGold where metricProviderId = 3 and dateStamp = '2021-10-28' 

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select sum(valueInDouble),avg(valueInDouble),min(valueInDouble),max(valueInDouble) from MetricHistoryGold where --dateStamp='2020-03-19' and hour = '08' 
// MAGIC unixTimestamp >=  1593561600 and unixTimestamp <  1596240000
// MAGIC and kymetaMetricId =44 and metricProviderId = 1
// MAGIC and remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e'

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY MetricGold

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY MetricSilver2

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY MetricCustomJoinSum

// COMMAND ----------

val InputDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Day")

// COMMAND ----------

import org.apache.spark.sql.functions._

display(InputDF.orderBy(desc("unixTimestamp")))

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricAggregatedByDay where unixTimestamp > 1593561600

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricAggregatedByMonth where unixTimestamp = 1598918400 and kymetaMetricId = 32

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricAggregatedByMonth where unixTimestamp > 1593561600

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and Datestamp = "2020-10-02"  
// MAGIC --and unixTimestamp = 1601580600
// MAGIC order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 2 and kymetaMetricId in (9003,9001,9002) --and Datestamp = "2020-09-25"  
// MAGIC and Datestamp = "2020-10-02"
// MAGIC order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC --select * from MetricGold where dateStamp = '2020-10-01' and metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and currentTimestamp >= 1601571301 
// MAGIC --order by unixTimestamp desc
// MAGIC 
// MAGIC select unixTimestamp, dateStamp, RemoteId, sum(valueInDouble ) from MetricGold where dateStamp = '2020-10-02' and metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and unixTimestamp >= 1601618100 and unixTimestamp <= 1601618100
// MAGIC group by unixTimestamp, dateStamp, metricProviderId, categoryId, RemoteId
// MAGIC order by unixTimestamp desc, RemoteId
// MAGIC --259339 : 2020-10-01T16:55:01.000+0000
// MAGIC -- timestamp 1601572800 to 1601588400

// COMMAND ----------

display(df.select("unixTimestamp", "RemoteId", "valueInDouble", "currentTimestamp").orderBy($"unixTimestamp".desc,$"remoteId"))

// COMMAND ----------

// MAGIC %sql
// MAGIC --select * from MetricGold where dateStamp = '2020-10-01' and metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and currentTimestamp >= 1601571301 
// MAGIC --order by unixTimestamp desc
// MAGIC 
// MAGIC select unixTimestamp, dateStamp, RemoteId, sum(valueInDouble )*1024 from MetricGold where (dateStamp = '2020-10-01' or dateStamp = '2020-10-02') and metricProviderId = 3 and kymetaMetricId in (212,213,214,215,216,217,218,219,220,221,222,223) and unixTimestamp >= 1601621215 and unixTimestamp <= 1601622565
// MAGIC group by unixTimestamp, dateStamp, metricProviderId, categoryId, RemoteId
// MAGIC order by unixTimestamp desc, RemoteId
// MAGIC --259339 : 2020-10-01T16:55:01.000+0000
// MAGIC -- timestamp 1601572800 to 1601588400

// COMMAND ----------

val df = spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-custom-sum")
  .filter($"metricProviderId" === 2 and $"kymetaMetricId" === 9003 )

display(df.select("unixTimestamp", "RemoteId", "valueInDouble", "currentTimestamp").orderBy($"unixTimestamp".desc,$"remoteId"))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricAggregatedByDay where unixTimestamp >= 1601510400 and kymetaMetricId = 44
// MAGIC order by unixTimestamp, remoteId

// COMMAND ----------

// MAGIC %sql
// MAGIC select unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd HH'),'yyyy-MM-dd HH') as UnixDay, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where unixTimestamp >= 1601510400 and kymetaMetricId = 44
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd HH'),'yyyy-MM-dd HH'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC order by UnixDay, remoteId

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
val InputDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .filter($"unixTimestamp" >= 1601510400 && $"kymetaMetricId" === 9003)
  .withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))

// COMMAND ----------

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggType = InputDF.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")


//val InputDFAggType = InputDF.join(dfMetricAggType).where(InputDF("kymetaMetricId") === dfMetricAggType("id") and ((dfMetricAggType("aggregationType") == "SUM") or (dfMetricAggType("aggregationType") == "AVG"))).select("unixDateStamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "value")


// COMMAND ----------

import org.apache.spark.sql.functions._
val aggregatesDF = InputDFAggType.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )


// COMMAND ----------

println(aggregatesDF.count())

// COMMAND ----------

// MAGIC %sql
// MAGIC desc MetricAggregatedByHour

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE MetricAggregatedByDay ADD COLUMNS (currentTimestamp bigint AFTER categoryId)

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE MetricAggregatedByMonth ADD COLUMNS (currentTimestamp bigint AFTER categoryId)

// COMMAND ----------

// MAGIC %sql
// MAGIC update MetricAggregatedByMonth set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE MetricAggregatedByHour ADD COLUMNS (currentTimestamp bigint AFTER categoryId)

// COMMAND ----------

// MAGIC %sql
// MAGIC update MetricAggregatedByHour set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 3 and Datestamp = "2020-10-06" 
// MAGIC and metric = "rtt" and valueInDouble < 0
// MAGIC order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricSilver2 where metricProviderId = 5 and Datestamp = "2020-10-26" --and metric = "rtt" and value > "0"
// MAGIC --and unix_timestamp = 1601780185 and value = '0'
// MAGIC order by unix_timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 5 and dateStamp = '2020-10-27' --and currentTimestamp = 1603139720 
// MAGIC and length(remoteId) < 30
// MAGIC order by currentTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC --desc MetricAggregatedByDay
// MAGIC select * from MetricAggregatedByDay where unixTimestamp =1602806400 --unixTimestamp >= 1601510400 --and kymetaMetricId = 9003 and sumValue != 0
// MAGIC order by remoteId, metricProviderId, kymetaMetricId
// MAGIC --update MetricAggregatedByDay set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC select unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd') as UnixDay, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where --unixTimestamp >= 1601510400 --and kymetaMetricId = 9003
// MAGIC dateStamp = '2020-10-16'
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0)-- and UnixDay = 1601942400)
// MAGIC order by UnixDay desc, remoteId, metricProviderId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC --desc MetricAggregatedByDay
// MAGIC select * from MetricAggregatedByMonth where unixTimestamp = 1601510400 and kymetaMetricId = 9003 
// MAGIC and sumValue != 0
// MAGIC order by unixTimestamp, remoteId, metricProviderId, kymetaMetricId
// MAGIC --update MetricAggregatedByDay set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC select  unix_timestamp(substring(dateStamp,0,7),'yyyy-MM') as UnixMonth, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where --unixTimestamp >= 1601510400 --and 
// MAGIC kymetaMetricId = 9003
// MAGIC group by unix_timestamp(substring(dateStamp,0,7),'yyyy-MM'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0 and UnixMonth = 1601510400)
// MAGIC order by UnixMonth, remoteId, metricProviderId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC --desc MetricAggregatedByDay
// MAGIC select * from MetricAggregatedByHour where unixTimestamp = 1602802800 --and kymetaMetricId = 44 
// MAGIC --and sumValue != 0
// MAGIC order by unixTimestamp desc, remoteId, metricProviderId, kymetaMetricId
// MAGIC --update MetricAggregatedByDay set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC select  unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd HH'),'yyyy-MM-dd HH') as UnixHour, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where dateStamp = '2020-10-15' and unixTimestamp >= 1602802800 and unixTimestamp < 1602082800 --and kymetaMetricId = 44
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd HH'),'yyyy-MM-dd HH'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0)
// MAGIC order by UnixHour desc, remoteId, metricProviderId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC select model, count(*) from satelliterouter
// MAGIC group by model

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from satelliterouter where serialnumber = '3034' order by RowKey

// COMMAND ----------

// MAGIC %sql
// MAGIC select serialnumber from satelliterouter where model = 'idirectx7'

// COMMAND ----------

// MAGIC %sql
// MAGIC select Model, count(*) from RemoteDeviceHistory
// MAGIC group by Model

// COMMAND ----------

// MAGIC %sql
// MAGIC select ModelType, count(*) from LeptonUniqueId
// MAGIC group by ModelType

// COMMAND ----------

// MAGIC %sql
// MAGIC select ModemSn from LeptonUniqueId where ModelType='idirectiq200'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from LeptonUniqueId where --NetModemId = 1013
// MAGIC ModemSn in ('21160','18322','52749','8362') order by ModemSn, NetModemId

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from RemoteDeviceHistory
// MAGIC where Serial in ('21160','18322','52749','8362')
// MAGIC order by Serial
// MAGIC --where Type = 'DEV_CLRTR'
// MAGIC --where Serial = '54925'

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct remoteId from MetricGold where metricProviderId = 6 and Datestamp >= "2020-10-21" --and remoteId = '7788fb5b-f63e-4c89-a053-d9ba4bb8c7fb' --'4363f6e0-7592-42b5-b1ba-361bd7358935'
// MAGIC --and remoteId = '4363f6e0-7592-42b5-b1ba-361bd7358935'
// MAGIC and kymetaMetricId = 203 and valueInString = 1
// MAGIC --order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct serialnumber from MetricSilver2 where metricProviderId = 6 and Datestamp >= "2020-10-21"  --and metric = "rtt" and value > "0"
// MAGIC --and unix_timestamp = 1601780185 and value = '0'
// MAGIC and metric = 'CellConnectionStatus' 
// MAGIC --and metric in ('cellular_signals_rssi','cellular_signals_sinr','cellular_signals_rsrp','cellular_signals_rsrq','cellular_signal_bar','cellular_carrier_name','cellular_sims_status','cellular_sims_apn','CellConnectionStatus') 
// MAGIC --and serialnumber = "2937-E951-7AC3"
// MAGIC and value = '1' 
// MAGIC --and serialnumber = '2937-E960-49D1'
// MAGIC --order by unix_timestamp desc, serialnumber, metric

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from PeplinkBronze 
// MAGIC order by timestamp desc
// MAGIC limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from PeplinkBronze where Datestamp = "2020-10-15" --and serial = "2937-E938-1C5C" and timestamp = '1602778621'
// MAGIC --and interfacesMsg is not null
// MAGIC order by timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 4 --and currentTimestamp = 1603139720 
// MAGIC order by currentTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricSilver2 where metricProviderId = 4 --and Datestamp = "2020-10-19" --and metric = "rtt" and value > "0"
// MAGIC --and unix_timestamp = 1601780185 and value = '0'
// MAGIC --and metric = 'CellConnectionStatus' 
// MAGIC --and metric in ('cellular_signals_rssi','cellular_signals_sinr','cellular_signals_rsrp','cellular_signals_rsrq','cellular_signal_bar','cellular_carrier_name','cellular_sims_status','cellular_sims_apn','CellConnectionStatus') 
// MAGIC --and value = '1' 
// MAGIC --and serialnumber = '2937-E960-49D1'
// MAGIC order by unix_timestamp desc --, serialnumber, metric

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from CubicBronze --where EnqueuedTime > '2020-10-19'
// MAGIC order by EnqueuedTime desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId in (1,2,3) and remoteId = '7788fb5b-f63e-4c89-a053-d9ba4bb8c7fb' 
// MAGIC --order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC --desc LeptonUniqueId
// MAGIC select * from LeptonUniqueId where ModemSn = '21160'

// COMMAND ----------

val evolutionDeltaDf = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmapping").select("NetModemId","ModemSn","ModelType")
display(evolutionDeltaDf.filter($"ModemSn" === 4815))

// COMMAND ----------

// MAGIC %sql
// MAGIC --desc satelliterouter
// MAGIC select * from satelliterouter where serialnumber = '3540'

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val intelsatDf = spark.read.format("delta").load(basePath + "DeltaTable/IntelsatUsage-bronze").where($"Datestamp".startsWith("2020-12"))
//println(intelsatDf.count())

// COMMAND ----------

println(intelsatDf.select("terminalId","SSPCId","timestamp","bytesReceived","bytesTransmitted").dropDuplicates().count())

// COMMAND ----------

// MAGIC %sql
// MAGIC select  unix_timestamp(substring(dateStamp,0,7),'yyyy-MM') as UnixMonth, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where --unixTimestamp >= 1601510400 --and
// MAGIC unixTimestamp >= 1612137600 and unixTimestamp < 1614556800 and
// MAGIC kymetaMetricId = 9003 and remoteId = '01bcae4e-f8f9-4c89-9cdd-d8a8cb25f56e' and metricProviderId = 2
// MAGIC group by unix_timestamp(substring(dateStamp,0,7),'yyyy-MM'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0 and UnixMonth = 1612137600)
// MAGIC order by UnixMonth, remoteId, metricProviderId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from MetricAggregatedByMonth where unixTimestamp = 1612137600 and kymetaMetricId = 9003 and remoteId = '01bcae4e-f8f9-4c89-9cdd-d8a8cb25f56e' and metricProviderId = 2
// MAGIC and sumValue != 0
// MAGIC order by unixTimestamp, remoteId, metricProviderId, kymetaMetricId
// MAGIC --update MetricAggregatedByDay set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from MetricAggregatedByDay where kymetaMetricId = 9003 and remoteId = '01bcae4e-f8f9-4c89-9cdd-d8a8cb25f56e' and metricProviderId = 2
// MAGIC and unixTimestamp >= 1612137600 and unixTimestamp < 1614556800
// MAGIC and sumValue != 0
// MAGIC order by unixTimestamp, remoteId, metricProviderId, kymetaMetricId
// MAGIC --update MetricAggregatedByDay set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select metricProviderId, sum(sumValue) from MetricAggregatedByDay where kymetaMetricId = 9003 --and remoteId = '01bcae4e-f8f9-4c89-9cdd-d8a8cb25f56e' and metricProviderId = 2
// MAGIC and unixTimestamp >= 1614556800 and unixTimestamp < 1617235200
// MAGIC and sumValue != 0
// MAGIC group by metricProviderId
// MAGIC order by metricProviderId
// MAGIC --order by unixTimestamp, remoteId, metricProviderId, kymetaMetricId
// MAGIC --update MetricAggregatedByDay set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC select  metricProviderId, sum(valueInDouble) from MetricGold where --unixTimestamp >= 1601510400 --and
// MAGIC kymetaMetricId = 9003 
// MAGIC and unixTimestamp >= 1614556800 and unixTimestamp < 1617235200
// MAGIC group by metricProviderId
// MAGIC --having(sum(valueInDouble) != 0)
// MAGIC order by metricProviderId

// COMMAND ----------

// MAGIC %sql
// MAGIC select metricProviderId, sum(sumValue) from MetricAggregatedByMonth where kymetaMetricId = 9003 
// MAGIC and unixTimestamp >= 1614556800 and unixTimestamp < 1617235200
// MAGIC and sumValue != 0
// MAGIC group by metricProviderId
// MAGIC order by metricProviderId
// MAGIC --update MetricAggregatedByDay set currentTimestamp = 0

// COMMAND ----------

// MAGIC %sql
// MAGIC select unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd') as UnixDay, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where 
// MAGIC unixTimestamp >= 1617235200 --and unixTimestamp < 1614556800 and
// MAGIC kymetaMetricId = 9003 --and remoteId = '01bcae4e-f8f9-4c89-9cdd-d8a8cb25f56e' and metricProviderId = 2
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0)-- and UnixDay = 1601942400)
// MAGIC order by UnixDay, remoteId, metricProviderId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct remoteId) from MetricGold where metricProviderId = 1 --and dateStamp = '2021-03-29'

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct remoteId) from MetricAggregatedByMonth where metricProviderId = 6

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricSilver2 where Datestamp > "2021-03-20" 
// MAGIC and 
// MAGIC metricProviderId = 3 
// MAGIC and model in ("idirect9350")
// MAGIC order by unix_timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where Datestamp > "2021-03-20" 
// MAGIC and 
// MAGIC metricProviderId = 3 

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct(serialnumber, model) from MetricSilver2 where Datestamp > "2021-03-20" 
// MAGIC and 
// MAGIC metricProviderId = 3 
// MAGIC and model in ("iconnexe850mp","idirectx5","idirect9350","idirectiq200r")

// COMMAND ----------


val serialmapping = spark.sql("select distinct Serial as serialnumber, RemoteId as remote_id from RemoteDeviceHistory where Serial in ('36595','25105','134196','45018','180519','228434','45010','38811','44451','171812','28842','38699','52331','1981','38933','53134')")
display(serialmapping)

// COMMAND ----------

val dfSilver = spark.sql("select * from MetricSilver2 where Datestamp > '2021-03-20' and metricProviderId = 3 and serialnumber in ('36595','25105','134196','45018','180519','228434','45010','38811','44451','171812','28842','38699','52331','1981','38933','53134')")

val dfSilverSelected = dfSilver.select( "unix_timestamp","Datestamp", "serialnumber", "Kymeta_metricId")

// COMMAND ----------

val dfGold = spark.sql("select elementId as element, unixTimestamp as unix_timestamp, dateStamp as Datestamp, metric, kymetaMetricId as Kymeta_metricId, remoteId from MetricGold where Datestamp > '2021-03-20' and metricProviderId = 3 and RemoteId in ('6af42318-2dc7-4e75-9350-3c5e7a9dd693','e656ea05-b54b-4f0f-8edf-f04738928c05','5e62b6dd-bf3d-4d71-a38c-9895d4bfda48','fa20eb7b-a80c-4b4f-8866-836dadbf4468','0717b180-d218-413b-9b48-5a139b5add05','cf6c10ba-7d06-4c86-be8f-2bc68c671348','2aa9b942-15c3-4be7-a40c-9f1211fb37f0','68c1fb78-3613-4f39-94b7-520eddcca8be','820f5485-f4a7-486b-92e8-defb4b182162','6c7fa7d0-7eaf-4206-9869-b3647774d7d7','8abd2731-8cf4-4a1c-9cfc-252962611a6d','0c4fc0d6-f455-4a15-9250-176b8d8b6eb5','6e366f76-c93d-4a8e-b06a-552eff98d299','97bd92c1-5830-4928-aee2-23fce0b3b807','b3ddcf7c-5940-4063-8cc9-5a6ab58f7a8a','641911ff-c2a6-44b6-a544-9927774ad26f')").join(serialmapping).where($"remoteId" === $"remote_id")
.select("unix_timestamp","Datestamp", "serialnumber", "Kymeta_metricId")

// COMMAND ----------

val dfDiff = dfSilverSelected.except(dfGold)
.select($"unix_timestamp".as("unix_timestamp_diff"),$"Datestamp".as("Datestamp_diff"), $"serialnumber".as("serialnumber_diff"), $"Kymeta_metricId".as("Kymeta_metricId_diff"))

// COMMAND ----------

display(dfDiff)

// COMMAND ----------

val dfExtractSilver = dfSilver.join(dfDiff).where(dfSilver("Datestamp") === dfDiff("Datestamp_diff") and dfSilver("unix_timestamp") === dfDiff("unix_timestamp_diff") and dfSilver("serialnumber") === dfDiff("serialnumber_diff") and dfSilver("Kymeta_metricId") === dfDiff("Kymeta_metricId_diff"))
.select(dfSilver("element"), dfSilver("unix_timestamp"), dfSilver("Datestamp"), dfSilver("metric"), dfSilver("value"), dfSilver("serialnumber"), dfSilver("model"), dfSilver("deviceType"), dfSilver("Kymeta_metricId"), dfSilver("metricProviderId"))

// COMMAND ----------

dfExtractSilver
.write
.format("delta")
.partitionBy("Datestamp")
.mode("overwrite")
.save(basePath + "DeltaTable/testHistory/Metric-silver2-evo-newDevice")  

// COMMAND ----------



// COMMAND ----------

display(dfDiff)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where unixTimestamp = 1616976000 and 
// MAGIC  remoteId= 'b3ddcf7c-5940-4063-8cc9-5a6ab58f7a8a' and  metricProviderId = 3

// COMMAND ----------

// MAGIC %sql
// MAGIC select unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd') as UnixDay, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where 
// MAGIC unixTimestamp >= 1616284800 and unixTimestamp < 1618012800 and
// MAGIC remoteId = 'b3ddcf7c-5940-4063-8cc9-5a6ab58f7a8a' and metricProviderId = 3
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0)-- and UnixDay = 1601942400)
// MAGIC order by UnixDay, remoteId, metricProviderId, kymetaMetricId

// COMMAND ----------

val dfSilver = spark.sql("select * from MetricSilver2 where  Datestamp > '2021-07-09' and metricProviderId = 3 and serialnumber in ('4815')")

// COMMAND ----------

display(dfSilver)

// COMMAND ----------

val dfSilver = spark.sql("select * from MetricSilver2 where  Datestamp > '2021-07-12' and metricProviderId = 3 and serialnumber in ('4815')")

// COMMAND ----------

dfSilver
.write
.format("delta")
.partitionBy("Datestamp")
.mode("overwrite")
.save(basePath + "DeltaTable/testHistory/Metric-silver2-evo-4815")  

// COMMAND ----------

val dfEvoBronze = spark.sql("select * from EvoBronze where  Datestamp = '2021-07-07' and timestamp < 1626977021 and uniqueId in ('2041','2042','2044', '2043', '2061')")

// COMMAND ----------

val dfEvoBronzeMax = spark.sql("select max(timestamp) from EvoBronze where  Datestamp = '2021-10-15' ")
display(dfEvoBronzeMax)

// COMMAND ----------

val dfEvoBronzeMin = spark.sql("select min(timestamp) from EvoBronze where  Datestamp = '2021-10-25' ")
display(dfEvoBronzeMin)

// COMMAND ----------

display(dfEvoBronze.orderBy($"timestamp" desc))

// COMMAND ----------

val dfEvoBronze = spark.sql("select * from EvoBronze where  Datestamp = '2021-08-11' and timestamp >= 1628683199")

// COMMAND ----------

display(dfEvoBronze.orderBy($"timestamp" asc))

// COMMAND ----------

dfEvoBronze
.write
.format("delta")
.partitionBy("Datestamp")
.mode("overwrite")
.save(basePath + "DeltaTable/testHistory/Metric-Bronze-evo-95812-95719-0727")  

// COMMAND ----------

val dfEvoBronze1 = spark.sql("select * from EvoBronze where  Datestamp > '2021-07-12' and uniqueId in ('2045')")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from PeplinkBronze where serial='2938-BF2A-A48C'

// COMMAND ----------

val dfEvo = spark
.read
.format("delta")
.load(basePath + "DeltaTable/testHistory/Metric-silver2-evo-newDevice")  

// COMMAND ----------

display(dfEvo.filter($"serialnumber" === "2585"))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where remoteId = 'b8862f82-5500-4640-85a3-f92b101c4901' and  metricProviderId = 3 and DateStamp > '2021-07-07' and DateStamp < '2021-07-10'
// MAGIC order by unixTimestamp asc

// COMMAND ----------

val serialmapping = spark.sql("select * from RemoteDeviceHistory where Serial in ('95719','95812')")
display(serialmapping)

// COMMAND ----------

val providermapping = spark.sql("select * from MetricProviders")
display(providermapping)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from PeplinkBronze where Datestamp = "2021-08-16" --and timestamp = 1597753500 --
// MAGIC and timestamp = 1629126439 --and metric = 'bytesReceived'
// MAGIC --order by EnqueuedTime asc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where Datestamp = '2021-09-27' and metricProviderId = 3 and remoteId = '25e8121e-c300-49ec-b3c7-afbdfc97729f'
// MAGIC  and kymetaMetricId in (75)
// MAGIC  order by currentTimestamp desc, unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where Datestamp = '2021-09-27' and metricProviderId = 5 and remoteId = 'ABW000K210506408'
// MAGIC  and kymetaMetricId in (75)
// MAGIC  order by currentTimestamp desc, unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where  Datestamp = '2021-09-26' and metricProviderId = 6 and remoteId = '750da9ff-f52d-47af-8721-17a712eae6b8' 
// MAGIC and aggregationType in ('AVG','SUM')
// MAGIC  and kymetaMetricId in (200)
// MAGIC  order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where  Datestamp = '2021-09-22' and metricProviderId = 4 and unixTimestamp = 1632339964 and remoteId = '0bd44c77-16e4-4f90-a0a3-8e77d30c7794'
// MAGIC  and kymetaMetricId in (9003)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from cubicBronze where timestamp = 1632339964 and Datestamp = '2021-09-22'-- and serial = 'ABW000K210506408'
// MAGIC -- and kymetaMetricId = 200

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from cubicBronze where Datestamp = '2022-01-01' and ICCID in ('8942306000059265914','8942306000059267274','8942306000059274213','8942306000059266672','8942306000058897907','8942306000059268553','8942306000059269221','8942306000059267969','8942306000059266706','8942306000059268223','8942306000059266607','8942306000059265369')
// MAGIC -- and kymetaMetricId = 200

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from RemoteDeviceHistory where RemoteId = '750da9ff-f52d-47af-8721-17a712eae6b8'

// COMMAND ----------

// MAGIC %sql
// MAGIC --dup check
// MAGIC select kymetaMetricId,remoteId,metricProviderId,categoryId,unixTimestamp, count(*) from MetricCustomJoinSum  
// MAGIC --where kymetaMetricId not in (71,72,84,85)
// MAGIC where metricProviderId=3 and kymetaMetricId = 9003 and dateStamp > '2021-11-01' 
// MAGIC group by kymetaMetricId,remoteId,metricProviderId,categoryId,unixTimestamp
// MAGIC having count(*) > 1
// MAGIC order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricCustomJoinSum
// MAGIC where metricProviderId=3 and kymetaMetricId = 9003 and dateStamp = '2021-09-01' and remoteId = '8f17c863-e262-44cc-9b3e-5f5609ffa336' and unixTimestamp = 1630540760

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where  Datestamp >= '2021-12-01' and metricProviderId = 3 and remoteId = 'a62282ce-e427-4033-b3d7-7b2f82396909'
// MAGIC -- and kymetaMetricId in (9003)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricSilver2 where  Datestamp >= '2021-12-01' and metricProviderId = 3 and serialnumber = '63897'