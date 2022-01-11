// Databricks notebook source
// MAGIC %md ######6.0 Run configuration script

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable",
  mountPoint = "/mnt/DeltaTable",
  extraConfigs = Map("fs.azure.account.key.kymetabigdata.blob.core.windows.net" -> dbutils.secrets.get(scope = "kymeta-databricksInt-scope", key = "kymetaInt-key")))


// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://generic-kymeta-reporting@kymetabigdata.blob.core.windows.net/DeltaTable",
  mountPoint = "/mnt/DeltaTablekymetaReporting",
  extraConfigs = Map("fs.azure.account.key.kymetabigdata.blob.core.windows.net" -> dbutils.secrets.get(scope = "kymeta-databricksInt-scope", key = "kymetaInt-key")))


// COMMAND ----------

val Subscription = "/mnt/DeltaTablekymetaReporting/subscription"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS SubscriptionDB 
  USING DELTA 
  OPTIONS (path = "$Subscription")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from SubscriptionDB

// COMMAND ----------

val SubscriptionDf = spark.read.parquet("wasbs://generic-kymeta-reporting@kymetabigdata.blob.core.windows.net/subscription")
SubscriptionDf.printSchema

// COMMAND ----------

SubscriptionDf.write
  .format("delta")
  .mode("overwrite")
  .save("wasbs://generic-kymeta-reporting@kymetabigdata.blob.core.windows.net/DeltaTable/subscription")

// COMMAND ----------

//display(dbutils.fs.ls("/mnt/DeltaTable/Metric-gold-Agg-Day"))

// COMMAND ----------

val Metric_Agg_Day_delta = "/mnt/DeltaTable/Metric-gold-Agg-Day"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggDeltaTable 
  USING DELTA 
  OPTIONS (path = "$Metric_Agg_Day_delta")
  """)

// COMMAND ----------

// Load the bronze table for Intelsat Usage
val Intelsatusage_bronze = basePath + "DeltaTable/IntelsatUsage-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS IntelsatusageBronze 
  USING DELTA 
  OPTIONS (path = "$Intelsatusage_bronze")
  """)


// COMMAND ----------

// Load the bronze table for Hub Usage
val Hubusage_bronze = basePath + "DeltaTable/Hubusage-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubusageBronze 
  USING DELTA 
  OPTIONS (path = "$Hubusage_bronze")
  """)


// COMMAND ----------

// Load the bronze table for Hub Stats
val Hubstats_bronze = basePath + "DeltaTable/Hubstats-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatsBronze 
  USING DELTA 
  OPTIONS (path = "$Hubstats_bronze")
  """)


// COMMAND ----------

// Load the bronze table for Hub Status
val Hubstatus_bronze = basePath + "DeltaTable/Hubstatus-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatusBronze 
  USING DELTA 
  OPTIONS (path = "$Hubstatus_bronze")
  """)


// COMMAND ----------

// Load the bronze table for EVO
val Evo_bronze = basePath + "DeltaTable/EVO-bronze"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS EvoBronze 
  USING DELTA 
  OPTIONS (path = "$Evo_bronze")
  """)


// COMMAND ----------

// Load the silver 1 table for Intelsat Usage
val Intelsatusage_silver1 = basePath + "DeltaTable/IntelsatUsage-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS IntelsatusageSilver1 
  USING DELTA 
  OPTIONS (path = "$Intelsatusage_silver1")
  """)


// COMMAND ----------

// Load the silver 1 table for Hub Usage
val Hubusage_silver1 = basePath + "DeltaTable/Hubusage-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubusageSilver1 
  USING DELTA 
  OPTIONS (path = "$Hubusage_silver1")
  """)


// COMMAND ----------

// Load the silver 1 table for Hub Stats
val Hubstats_silver1 = basePath + "DeltaTable/Hubstats-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatsSilver1 
  USING DELTA 
  OPTIONS (path = "$Hubstats_silver1")
  """)


// COMMAND ----------

// Load the silver 1 table for Hub Status
val Hubstatus_silver1 = basePath + "DeltaTable/Hubstatus-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS HubstatusSilver1 
  USING DELTA 
  OPTIONS (path = "$Hubstatus_silver1")
  """)


// COMMAND ----------

// all the meta tables
val sspcpath = basePath + "DeltaTable/sspc"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS sspc 
  USING DELTA 
  OPTIONS (path = "$sspcpath")
  """)

val serviceplanpath = basePath + "DeltaTable/servicePlan"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS servicePlan 
  USING DELTA 
  OPTIONS (path = "$serviceplanpath")
  """)

val terminalpath = basePath + "DeltaTable/terminal"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS terminal 
  USING DELTA 
  OPTIONS (path = "$terminalpath")
  """)

val satelliterouterpath = basePath + "DeltaTable/satelliterouter"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS satelliterouter 
  USING DELTA 
  OPTIONS (path = "$satelliterouterpath")
  """)



// COMMAND ----------

// MAGIC %sql select * from IntelsatusageBronze where Datestamp='2020-03-19' and timestamp in ('1584610000','1584610300','1584610600','1584610900','1584611200','1584611500','1584611800','1584612100','1584612400','1584612700','1584613000','1584613300','1584613600','1584613900','1584614200','1584614500') 
// MAGIC and terminalId = '988720'
// MAGIC order by timestamp, SSPCId
// MAGIC --hour = '21'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select * from HubusageBronze where Datestamp='2020-03-19' and timestamp in ('1584610000','1584610300','1584610600','1584610900','1584611200','1584611500','1584611800','1584612100','1584612400','1584612700','1584613000') --hour = '22'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select count(*) from HubstatsBronze where Datestamp='2020-03-19' and timestamp in ('1584601300','1584601600','1584601900','1584602200','1584602500','1584605000','1584605300','1584605600','1584605900','1584606200') 
// MAGIC and metric = '1098' and element in ('988719','988720','988721', '988722', '988723')
// MAGIC --r = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select * from HubstatusBronze where datestamp = '2020-06-07' and timestamp in ('1591515000') 
// MAGIC and metric_id = '1668' and element_id in ('25475')
// MAGIC --r = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select * from HubstatusBronze where Datestamp='2020-03-19' and timestamp in ('1584601300', '1584602000','1584603000', '1584605000', '1584606000')
// MAGIC and metric_id = '1668'--hour = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select distinct table from EvoBronze --where --Datestamp='2020-07-04'
// MAGIC --table = 'raw_ip_stats'
// MAGIC --table = 'state_change_log'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricSilver2 where  Kymeta_metricId = 120 and metricProviderId = 3
// MAGIC order by unix_timestamp

// COMMAND ----------

// MAGIC %sql select * from IntelsatusageSilver1 where Datestamp='2020-03-19' and timestamp in ('1584610000','1584610300','1584610600','1584610900','1584611200','1584611500','1584611800','1584612100','1584612400','1584612700','1584613000','1584613300','1584613600','1584613900','1584614200','1584614500') 
// MAGIC and terminalId = '988720'
// MAGIC order by timestamp, SSPCId
// MAGIC --order by timestamp--hour = '21'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql 
// MAGIC select Datestamp, hour, count(*) from IntelsatusageSilver1 where timestamp >  '1596000000' 
// MAGIC group by Datestamp, hour
// MAGIC order by Datestamp, hour

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from IntelsatusageBronze where Datestamp='2020-03-04' and timestamp in ('1583294800','1583295100','1583295400','1583295700','1583296000','1583296300','1583296600','1583296900','1583297200','1583297500','1583297800','1583298100','1583298400','1583298700','1583299000','1583299300') 

// COMMAND ----------

// MAGIC %sql select * from HubusageSilver1 where timestamp in ('1593590700') 
// MAGIC and element in ('1498187','1498193','1498198') and metric in (1055)
// MAGIC --order by timestamp--hour = '22'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from sspc where RowKey_SSPC in ('1501691') 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from sspc where UsageName = 'DATA' and ObjName like '%MGMT%'

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from ServicePlan where RowKey_ServicePlan in ('33528') 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from terminal where RowKey in ('156359') 

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from satelliterouter where RowKey in ('156358') 

// COMMAND ----------

// MAGIC %sql select * from HubusageSilver1 where  timestamp in ('1583344000','1583344300','1583344600','1583344900','1583345200','1583345500','1583345800','1583346100','1583346400','1583346700','1583347000')

// COMMAND ----------

// MAGIC %sql select count(*) from HubusageBronze where Datestamp='2020-03-04' and timestamp in ('1583344000','1583344300','1583344600','1583344900','1583345200','1583345500','1583345800','1583346100','1583346400','1583346700','1583347000')

// COMMAND ----------

// MAGIC %sql select * from HubstatsSilver1 where timestamp in ('1590706532','1590704432') 
// MAGIC --and metric = '1098' and element in ('988719','988720','988721', '988722', '988723') order by timestamp  -- hour = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select count(*) from HubstatsSilver1 where timestamp in ('1583344000','1583344300','1583344600','1583344900','1583345200','1583345500','1583345800','1583346100','1583346400','1583346700','1583347000','1583347300') 
// MAGIC -- hour = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select count(*) from HubstatsBronze where Datestamp='2020-03-04' and timestamp in ('1583344000','1583344300','1583344600','1583344900','1583345200','1583345500','1583345800','1583346100','1583346400','1583346700','1583347000','1583347300') 
// MAGIC -- hour = '19'
// MAGIC -- hour = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select count(*) from HubstatsSilver1 where datestamp = '2020-07-31' and timestamp > '1596237000' and timestamp < '1596238500'
// MAGIC --and metric = '1093'
// MAGIC 
// MAGIC --and metric_id = '1668' and element_id in ('25475')
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// MAGIC %sql select * from HubstatusSilver1 where Datestamp='2020-09-13' --and timestamp='1583294800'  --hour = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'
// MAGIC order by EnqueuedTime desc

// COMMAND ----------

// MAGIC %sql select count(*) from HubstatusBronze where Datestamp='2020-03-04' and timestamp='1583294800' --hour = '19'
// MAGIC -- terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// Load the silver 2 table
val Metric_silver2 = basePath + "DeltaTable/Metric-silver-step2"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricSilver2 
  USING DELTA 
  OPTIONS (path = "$Metric_silver2")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from MetricSilver2 where unix_timestamp > '1596237000' and unix_timestamp < '1596238500'  and metric = '1093'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from MetricSilver2 where unix_timestamp in ('1593617100')  and Kymeta_metricId in (71,72) and metricProviderId in (2)
// MAGIC order by Kymeta_metricId, serialnumber

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from MetricSilver2 where unix_timestamp > '1593650000'  and  metricProviderId in (3)
// MAGIC order by unix_timestamp

// COMMAND ----------

val factModem = spark.read
  .format("delta")
  .load(basePath+"DeltaTable/remotedevicehistory")
display(factModem.filter($"serial" === "ABC000K200803105"))


// COMMAND ----------

// Load the gold table
val Metric_gold = basePath + "DeltaTable/Metric-gold-raw"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricGold 
  USING DELTA 
  OPTIONS (path = "$Metric_gold")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select * from MetricGold where unixTimestamp > '1596237000' and unixTimestamp < '1596238500'  and KymetaMetricId = 44

// COMMAND ----------

// MAGIC %sql
// MAGIC --dup check
// MAGIC select kymetaMetricId,remoteId,metricProviderId,categoryId,unixTimestamp, count(*) from MetricGold  
// MAGIC --where kymetaMetricId not in (71,72,84,85)
// MAGIC where metricProviderId not in (3)
// MAGIC group by kymetaMetricId,remoteId,metricProviderId,categoryId,unixTimestamp
// MAGIC having count(*) > 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricSilver2 where unix_timestamp = '1596051000' and serialnumber = 57369 and Kymeta_metricId in (71,96) and metricProviderId =1
// MAGIC order by Kymeta_metricId, serialnumber

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC 
// MAGIC select count(*) from MetricGold where metricProviderId in (3) and unixTimestamp > 1593650000
// MAGIC 
// MAGIC --kymetaMetricId in (9001,9002,9003,9004)
// MAGIC 
// MAGIC --delete from MetricGold where metricProviderId = 1 and kymetaMetricId in (9001,9002,9003,9004)

// COMMAND ----------

// Load the gold table
val Metric_gold_custom_join = basePath + "DeltaTable/Metric-gold-custom-join"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricCustomJoinGold 
  USING DELTA 
  OPTIONS (path = "$Metric_gold_custom_join")
  """)


// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC 
// MAGIC --select * from MetricCustomJoinGold where kymetaMetricId in (9004) order by remoteId, unixTimestamp asc
// MAGIC select count(*) from MetricCustomJoinGold

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where 
// MAGIC dateStamp = '2020-06-07' and unixTimestamp in ('1591550800') 
// MAGIC --and metric = '1668' and elementId in ('25475')

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricGold where remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1')
// MAGIC --valueInDouble is not null --unixTimestamp = 1586982638 and remoteId = 'fc4fd5f0-df89-4179-8caf-4c5afeef06b1'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1') order by unixTimestamp 
// MAGIC --valueInDouble is not null --unixTimestamp = 1586982638 and remoteId = 'fc4fd5f0-df89-4179-8caf-4c5afeef06b1'

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricGold where unixTimestamp >= 1586300000 --and metric = 1093
// MAGIC and remoteId in ('8e379257-99d3-4509-932c-1fd765edaeeb','b86b3149-d3b4-48db-a0af-3593429fd79e','268257fa-2cfd-4c9e-bfd5-5471b39b9145','c4f51407-dee4-46a3-8d96-4fec2c23926a','157912ae-dd99-4251-adb3-7c25fa4d8483','beaadba2-7970-4167-9a78-375001e472eb','7fdb5576-54e4-4eb6-8277-a85dfe4dfced','67211153-5569-4a53-aab1-2ee4403731a7','1764b9f3-1e92-4f9d-969d-d13a84b0f889','866d5a64-c2ae-44d2-8264-96743857dbaf')

// COMMAND ----------

// MAGIC %sql
// MAGIC select unixTimestamp,RemoteId, sum(valueInDouble) from MetricGold where kymetaMetricId in (71,72,84,85) and metricProviderId = 2
// MAGIC group By unixTimestamp, RemoteId
// MAGIC order by unixTimestamp, remoteId

// COMMAND ----------

// MAGIC %sql
// MAGIC select unixTimestamp,RemoteId, kymetaMetricId, valueInDouble from MetricGold where kymetaMetricId in (130,131) and unixTimestamp in (1585890000,1585890300,1585890600)
// MAGIC --group By unixTimestamp, RemoteId
// MAGIC order by unixTimestamp, remoteId, kymetaMetricId

// COMMAND ----------

// Load the gold table
val Metric_gold_custom = basePath + "DeltaTable/Metric-gold-custom"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricGoldCustom 
  USING DELTA 
  OPTIONS (path = "$Metric_gold_custom")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC select unixTimestamp,RemoteId,  valueInDouble from MetricGoldCustom where 
// MAGIC kymetaMetricId in (9003) and metricProviderId = 2
// MAGIC order by unixTimestamp, remoteId

// COMMAND ----------

// MAGIC %sql
// MAGIC select unixTimestamp,RemoteId,  valueInString from MetricGoldCustom where 
// MAGIC kymetaMetricId in (9004) --and metricProviderId = 2
// MAGIC and unixTimestamp in (1585890000,1585890300,1585890600)
// MAGIC order by unixTimestamp, remoteId

// COMMAND ----------

// MAGIC %sql
// MAGIC --Hubstatus
// MAGIC select * from MetricSilver2 where Datestamp = '2020-06-10' and unix_timestamp in ('1591810000','1591815000') 
// MAGIC --and metric = '1668' and element in ('25475')

// COMMAND ----------

// MAGIC %sql
// MAGIC --ASM
// MAGIC select * from MetricSilver2 where 
// MAGIC serialnumber = 'AAE000J171016171' and unix_timestamp in ('1578695212')
// MAGIC --Datestamp='2020-03-19' and unix_timestamp in ('1584601300','1584602000','1584603000','1584605000','1584606000','1584603600','1584606600')
// MAGIC --and metric = '1668'
// MAGIC --order by element

// COMMAND ----------

// MAGIC %sql
// MAGIC --Hubstats
// MAGIC select count(*) from MetricSilver2 where Datestamp='2020-03-18' and unix_timestamp in ('1584510000','1584510300','1584510600','1584510900','1584511200','1584511500','1584511800','1584512100','1584512400','1584512700','1584513000','1584513300') and metric = '1098' and element in ('988719','988720','988721', '988722', '988723')

// COMMAND ----------

// MAGIC %sql
// MAGIC --Hubusage
// MAGIC select * from MetricSilver2 where Datestamp='2020-03-19' and Hour = '07'
// MAGIC and unix_timestamp in ('1584601300','1584601600','1584601900','1584602200','1584602500','1584605000','1584605300','1584605600','1584605900','1584606200','1584606500')
// MAGIC and metric in ('1054','1055')
// MAGIC and element in ('988750','988752','988754', '988756', '988758', '988760', '988762')
// MAGIC --and element in ('988750','988752','988754', '988756', '988758', '988760', '988762', '988725')

// COMMAND ----------

// MAGIC %sql
// MAGIC --Intelsatusage
// MAGIC select count(*) from MetricSilver2 where Datestamp='2020-03-18' 
// MAGIC and unix_timestamp in ('1584510000','1584510300','1584510600','1584510900','1584511200','1584511500','1584511800','1584512100','1584512400','1584512700','1584513000','1584513300','1584513600','1584513900','1584514200','1584514500') 
// MAGIC  and metric in ('bytesReceived','bytesTransmitted')
// MAGIC --order by unix_timestamp

// COMMAND ----------

// MAGIC %sql
// MAGIC --Intelsatusage
// MAGIC select substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId, sum(value),avg(value),min(value),max(value) from MetricGold where --dateStamp='2020-03-19' and hour = '08'
// MAGIC --unixTimestamp in ('1584701300','1584706300','1584750000','1584760000','1584850000','1584870000','1584890000','1584895000','1584955000','1584955800')
// MAGIC --dateStamp in ('2020-03-20','2020-03-21','2020-03-22','2020-03-23') and
// MAGIC  metric in ('bytesReceived','bytesTransmitted')
// MAGIC  and remoteId in ('c4f51407-dee4-46a3-8d96-4fec2c23926a',
// MAGIC '157912ae-dd99-4251-adb3-7c25fa4d8483',
// MAGIC '7fdb5576-54e4-4eb6-8277-a85dfe4dfced',
// MAGIC '866d5a64-c2ae-44d2-8264-96743857dbaf',
// MAGIC '8e379257-99d3-4509-932c-1fd765edaeeb',
// MAGIC 'b86b3149-d3b4-48db-a0af-3593429fd79e',
// MAGIC '268257fa-2cfd-4c9e-bfd5-5471b39b9145',
// MAGIC 'beaadba2-7970-4167-9a78-375001e472eb',
// MAGIC '67211153-5569-4a53-aab1-2ee4403731a7',
// MAGIC '1764b9f3-1e92-4f9d-969d-d13a84b0f889'
// MAGIC ) 
// MAGIC group by substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId
// MAGIC order by substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId
// MAGIC --order by unixTimestamp

// COMMAND ----------

// MAGIC %sql
// MAGIC --Hubstatus
// MAGIC select substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId, sum(value),avg(value),min(value),max(value) from MetricGold where dateStamp like '2020-03%' --and hour = '08'
// MAGIC --and unixTimestamp in ('1582700000','1582800000','1582830000','1582835000','1582838000','1582848000','1582855500','1582856500','1582938500','1582948500')
// MAGIC and metric = '1668' and kymetaMetricid = 116
// MAGIC group by substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId
// MAGIC order by substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId

// COMMAND ----------

// MAGIC %sql
// MAGIC --Hubusage
// MAGIC select substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId, sum(value),avg(value),min(value),max(value) from MetricGold where --dateStamp='2020-03-19' and hour = '08' 
// MAGIC --unixTimestamp in ('1584701300','1584706300','1584750000','1584760000','1584850000','1584870000','1584890000','1584895000','1584955000','1584955800') 
// MAGIC --dateStamp in ('2020-03-20','2020-03-21','2020-03-22','2020-03-23') and
// MAGIC  remoteId in ('c4f51407-dee4-46a3-8d96-4fec2c23926a',
// MAGIC '157912ae-dd99-4251-adb3-7c25fa4d8483',
// MAGIC '7fdb5576-54e4-4eb6-8277-a85dfe4dfced',
// MAGIC '866d5a64-c2ae-44d2-8264-96743857dbaf',
// MAGIC '8e379257-99d3-4509-932c-1fd765edaeeb',
// MAGIC 'b86b3149-d3b4-48db-a0af-3593429fd79e',
// MAGIC '268257fa-2cfd-4c9e-bfd5-5471b39b9145',
// MAGIC 'beaadba2-7970-4167-9a78-375001e472eb',
// MAGIC '67211153-5569-4a53-aab1-2ee4403731a7',
// MAGIC '1764b9f3-1e92-4f9d-969d-d13a84b0f889'
// MAGIC ) 
// MAGIC and kymetaMetricId in (71,72,73,84,85,86) and metricProviderId = 1
// MAGIC group by substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId
// MAGIC order by substring(dateStamp,0,7), kymetaMetricId, remoteId, metricProviderId

// COMMAND ----------

// MAGIC %sql
// MAGIC --Hubstats
// MAGIC select substring(dateStamp,0,7), remoteId, sum(value),avg(value),min(value),max(value) from MetricGold where --dateStamp='2020-03-19' and hour = '08'
// MAGIC --unixTimestamp in ('1584701300','1584706300','1584750000','1584760000','1584850000','1584870000','1584890000','1584895000','1584955000','1584955800') 
// MAGIC metric = '1098' and elementId in ('988719','988720','988721', '988722', '988723') and
// MAGIC remoteId in ('c4f51407-dee4-46a3-8d96-4fec2c23926a',
// MAGIC '157912ae-dd99-4251-adb3-7c25fa4d8483',
// MAGIC '7fdb5576-54e4-4eb6-8277-a85dfe4dfced',
// MAGIC '866d5a64-c2ae-44d2-8264-96743857dbaf',
// MAGIC '8e379257-99d3-4509-932c-1fd765edaeeb',
// MAGIC 'b86b3149-d3b4-48db-a0af-3593429fd79e',
// MAGIC '268257fa-2cfd-4c9e-bfd5-5471b39b9145',
// MAGIC 'beaadba2-7970-4167-9a78-375001e472eb',
// MAGIC '67211153-5569-4a53-aab1-2ee4403731a7',
// MAGIC '1764b9f3-1e92-4f9d-969d-d13a84b0f889'
// MAGIC ) 
// MAGIC group by substring(dateStamp,0,7), remoteId
// MAGIC order by substring(dateStamp,0,7), remoteId

// COMMAND ----------

// MAGIC %sql
// MAGIC --ASM
// MAGIC select substring(dateStamp,0,7), remoteId, kymetaMetricId, sum(valueInDouble),avg(valueInDouble),min(valueInDouble),max(valueInDouble) from MetricGold where 
// MAGIC valueInDouble is not null and 
// MAGIC remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1') 
// MAGIC group by substring(dateStamp,0,7), remoteId, kymetaMetricId
// MAGIC order by substring(dateStamp,0,7), remoteId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC --ASM
// MAGIC select unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd HH'),'yyyy-MM-dd HH') as UnixHour, remoteId, kymetaMetricId, sum(valueInDouble),avg(valueInDouble),min(valueInDouble),max(valueInDouble) from MetricGold where 
// MAGIC valueInDouble is not null and 
// MAGIC remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1') 
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,"yyyy-MM-dd HH"),"yyyy-MM-dd HH"), remoteId, kymetaMetricId
// MAGIC order by unix_timestamp(from_unixtime(unixTimestamp,"yyyy-MM-dd HH"),"yyyy-MM-dd HH"), remoteId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC --ASM
// MAGIC select unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM'),'yyyy-MM') as UnixMonth, remoteId, kymetaMetricId, sum(valueInDouble),avg(valueInDouble),min(valueInDouble),max(valueInDouble) from MetricGold where 
// MAGIC valueInDouble is not null and 
// MAGIC remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1') 
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,"yyyy-MM"),"yyyy-MM"), remoteId, kymetaMetricId
// MAGIC order by unix_timestamp(from_unixtime(unixTimestamp,"yyyy-MM"),"yyyy-MM"), remoteId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC --ASM
// MAGIC select remoteId, kymetaMetricId, metricProviderId, categoryId, max(unixTimestamp) from MetricGold where remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1')
// MAGIC 
// MAGIC group by remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC order by max(unixTimestamp), remoteId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select sum(valueInDouble),avg(valueInDouble),min(valueInDouble),max(valueInDouble) from MetricGold where --dateStamp='2020-03-19' and hour = '08' 
// MAGIC unixTimestamp >=  1593561600 and unixTimestamp <  1596240000
// MAGIC and kymetaMetricId =44 and metricProviderId = 1
// MAGIC and remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e'

// COMMAND ----------

val InputDF = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-Agg-Day")


// COMMAND ----------

display(InputDF.filter($"unixTimestamp" > 1593561600))

// COMMAND ----------

// MAGIC %sql
// MAGIC describe HISTORY MetricGold 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 2 and kymetaMetricId = 9003 -- and unixTimestamp > 1598918400
// MAGIC --select * from MetricGold where metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and unixTimestamp > 1598918400
// MAGIC order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 3 and kymetaMetricId in (214,215,216,217,218,219,220,231,232,233)
// MAGIC 
// MAGIC --unixTimestamp = 1600797395 and remoteId = 'b546bbed-5798-4ab3-b878-d7f0621fe24e'

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC select remoteId, Datestamp, kymetaMetricId, metricProviderId, categoryId, unixTimestamp, valueInDouble, valueInString from MetricGold where Datestamp = '2020-06-10' and unixTimestamp in ('1591810000','1591815000') 

// COMMAND ----------

// MAGIC %sql
// MAGIC --EVO
// MAGIC select * from MetricGold where metricProviderId = 3--remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1')
// MAGIC --and Datestamp = '2020-06-07'
// MAGIC and unixTimestamp > '1591500000'

// COMMAND ----------

// MAGIC %sql
// MAGIC --EVO
// MAGIC select * from MetricSilver2 where metricProviderId = 3
// MAGIC and unix_timestamp > '1591500000'
// MAGIC --and unix_timestamp = '1591561598'
// MAGIC --order by unix_timestamp

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC select remoteId, kymetaMetricId, metricProviderId, categoryId, max(unixTimestamp) from MetricGold where remoteId in ('8e379257-99d3-4509-932c-1fd765edaeeb','b86b3149-d3b4-48db-a0af-3593429fd79e','268257fa-2cfd-4c9e-bfd5-5471b39b9145','c4f51407-dee4-46a3-8d96-4fec2c23926a','157912ae-dd99-4251-adb3-7c25fa4d8483','beaadba2-7970-4167-9a78-375001e472eb','7fdb5576-54e4-4eb6-8277-a85dfe4dfced','67211153-5569-4a53-aab1-2ee4403731a7','1764b9f3-1e92-4f9d-969d-d13a84b0f889','866d5a64-c2ae-44d2-8264-96743857dbaf')
// MAGIC and unixTimestamp >= 1586300000
// MAGIC group by remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC order by remoteId,max(unixTimestamp)

// COMMAND ----------

// Load the Agg by Hour table
val metric_gold_Agg_Hour = basePath + "DeltaTable/Metric-gold-Agg-Hour"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggregatedByHour 
  USING DELTA 
  OPTIONS (path = "$metric_gold_Agg_Hour")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricAggregatedByHour where unixTimestamp >= 1593561600

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
// Read the raw gold table as stream
val InputDFHour = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM-dd HH"),"yyyy-MM-dd HH"))
  .select("unixTimestamp", "valueInDouble",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggType = InputDFHour.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

val aggregatesDFHour = InputDFAggType.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )



// COMMAND ----------

println(aggregatesDFHour.count())

// COMMAND ----------

// Load the Agg by Hour table
val metric_gold_Agg_Day = basePath + "DeltaTable/Metric-gold-Agg-Day"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggregatedByDay 
  USING DELTA 
  OPTIONS (path = "$metric_gold_Agg_Day")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricAggregatedByDay where unixTimestamp >= 1593561600

// COMMAND ----------

val InputDFDay = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw").drop("unixTimestamp")
  .withColumn("unixTimestamp", unix_timestamp($"dateStamp","yyyy-MM-dd"))

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggType = InputDFDay.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

val aggregatesDFDay = InputDFAggType.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )



// COMMAND ----------

println(aggregatesDFDay.count())

// COMMAND ----------

// Load the Agg by Hour table
val metric_gold_Agg_Month = basePath + "DeltaTable/Metric-gold-Agg-Month"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricAggregatedByMonth 
  USING DELTA 
  OPTIONS (path = "$metric_gold_Agg_Month")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from MetricAggregatedByMonth

// COMMAND ----------

val InputDFMonth = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("unixTimestamp", unix_timestamp(from_unixtime($"unixTimestamp","yyyy-MM"),"yyyy-MM"))
  .select("unixTimestamp", "valueInDouble",  "kymetaMetricId", "remoteId", "metricProviderId", "categoryId")

val dfMetricAggType = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/metrics-aggtype")

val InputDFAggType = InputDFMonth.join(
dfMetricAggType,
    expr(""" 
      kymetaMetricId = id and (aggregationType = 'SUM' or aggregationType = 'AVG')
      """
    )
  )
.select("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId", "categoryId", "valueInDouble")

val aggregatesDFMonth = InputDFAggType.groupBy("unixTimestamp", "remoteId", "kymetaMetricId", "metricProviderId","categoryId")
.agg((sum($"valueInDouble")).alias("sumValue"),(avg($"valueInDouble")).alias("avgValue"), (min($"valueInDouble")).alias("minValue"),(max($"valueInDouble")).alias("maxValue") )



// COMMAND ----------

println(aggregatesDFMonth.count())

// COMMAND ----------

// Load the Agg by Hour table
val metric_gold_LatestRaw = basePath + "DeltaTable/Metric-gold-latest"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricLatest 
  USING DELTA 
  OPTIONS (path = "$metric_gold_LatestRaw")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC --latest metric
// MAGIC select count(*) from MetricLatest  

// COMMAND ----------

// MAGIC %sql
// MAGIC --ASM
// MAGIC --select * from MetricLatest where kymetaMetricId = 23 and remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e' order by unixTimestamp desc
// MAGIC select * from MetricLatest where remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e' order by unixTimestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC --select * from MetricGold where unixTimestamp = 1597183800 and metric in (1067,1316,1133,1067) and elementId in (1484126,600604)
// MAGIC -- select * from MetricGold where unixTimestamp in (1597183920,1597187520,1597144620,1597242780) and metric in (1067,1316,1133,1067,1318) and elementId in (1484126,600604)
// MAGIC --select max(unixTimestamp)  from MetricGold where kymetaMetricId = 23 and metricProviderId = 1 and remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e' and unixTimestamp >= 1600186200
// MAGIC --order by unixTimestamp desc limit 10
// MAGIC select max(unixTimestamp)  from MetricGold where unixTimestamp >= 1600186200

// COMMAND ----------

// MAGIC %sql
// MAGIC --latest
// MAGIC --select * from MetricLatest where kymetaMetricId = 23 and remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e' and metricProviderId=1 --order by unixTimestamp desc
// MAGIC --select * from MetricAggLatest where remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e' and kymetaMetricId = 23 and metricProviderId=1
// MAGIC --latest
// MAGIC select count(*) from MetricLatest  where unixTimestamp >= 1600186200 --order by unixTimestamp desc
// MAGIC --select * from MetricAggLatest where remoteId = 'ef1c30e1-7e2f-484c-8321-8378bca3c49e' and kymetaMetricId = 23 and metricProviderId=1

// COMMAND ----------

val dfLatestMetricStore = spark.read
  .format("delta")
 // .option("ignoreChanges", "true")
  .load(basePath + "DeltaTable/Metric-latest-store")

// COMMAND ----------

display(dfLatestMetricStore.filter($"remoteId" === "ef1c30e1-7e2f-484c-8321-8378bca3c49e" && $"kymetaMetricId" === 118))

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val GoldDF1 = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/Metric-gold-raw")
  .withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
  .filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 30)))

val OutputDf1 = 
GoldDF1.groupBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId")
.agg((max($"unixTimestamp")).alias("maxUnixTimestamp"))


// COMMAND ----------

println(OutputDf1.count())

// COMMAND ----------

display(OutputDf1.filter($"metricProviderId" === 5).orderBy("remoteId", "kymetaMetricId", "metricProviderId", "categoryId"))

// COMMAND ----------

// MAGIC %sql
// MAGIC --ASM
// MAGIC select remoteId, maxUnixTimestamp, kymetaMetricId, MetricProviderId, categoryId from MetricLatestRaw where remoteId in ('19afee16-a277-4bf8-900d-fcf129865def','97abd2da-9b98-4c53-baee-04af0a904c9e','a370c271-1b0a-4745-b55b-de0da2cc3f40','fc4fd5f0-df89-4179-8caf-4c5afeef06b1')
// MAGIC order by maxUnixTimestamp, remoteId, kymetaMetricId, MetricProviderId, categoryId

// COMMAND ----------

// MAGIC %sql
// MAGIC --EVO
// MAGIC select * from MetricAggLatest where metricProviderId = 3 order by unixTimestamp

// COMMAND ----------

// MAGIC %sql
// MAGIC --EVO
// MAGIC select * from MetricAggMonth where metricProviderId = 3

// COMMAND ----------

// MAGIC %sql
// MAGIC --EVO
// MAGIC select * from MetricAggDay where metricProviderId = 3

// COMMAND ----------

// MAGIC %sql
// MAGIC --EVO
// MAGIC select * from MetricAggHour where metricProviderId = 3

// COMMAND ----------

// MAGIC %sql
// MAGIC select remoteId, maxUnixTimestamp, kymetaMetricId, MetricProviderId, categoryId from MetricLatestRaw where remoteId in ('8e379257-99d3-4509-932c-1fd765edaeeb','b86b3149-d3b4-48db-a0af-3593429fd79e','268257fa-2cfd-4c9e-bfd5-5471b39b9145','c4f51407-dee4-46a3-8d96-4fec2c23926a','157912ae-dd99-4251-adb3-7c25fa4d8483','beaadba2-7970-4167-9a78-375001e472eb','7fdb5576-54e4-4eb6-8277-a85dfe4dfced','67211153-5569-4a53-aab1-2ee4403731a7','1764b9f3-1e92-4f9d-969d-d13a84b0f889','866d5a64-c2ae-44d2-8264-96743857dbaf')
// MAGIC and maxUnixTimestamp >= 1586300000
// MAGIC order by remoteId, maxUnixTimestamp, kymetaMetricId, MetricProviderId, categoryId

// COMMAND ----------

// Load the metric mapping table
val Metric_mapping = basePath + "DeltaTable/metricmappings"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS MetricMapping 
  USING DELTA 
  OPTIONS (path = "$Metric_mapping")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC --MetricMapping
// MAGIC select * from MetricMapping where metricId in (75)

// COMMAND ----------

// Load the metric mapping table
val Device_History = basePath + "DeltaTable/remotedevicehistory"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS RemoteDeviceHistory 
  USING DELTA 
  OPTIONS (path = "$Device_History")
  """)

// COMMAND ----------

// Load the metric mapping table
val leptonUniqueId = basePath + "DeltaTable/uniqueidmapping"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS LeptonUniqueId 
  USING DELTA 
  OPTIONS (path = "$leptonUniqueId")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from LeptonUniqueId --where NetModemId = 758

// COMMAND ----------

// MAGIC %sql
// MAGIC --DeviceHistory
// MAGIC select * from RemoteDeviceHistory where Serial in ('1782','39489','54962','56708','57369','57600')

// COMMAND ----------

// MAGIC %sql
// MAGIC select ModemSn, count(*)
// MAGIC  FROM LeptonUniqueId
// MAGIC group by ModemSn
// MAGIC having count(*) > 1

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC       RemoteId
// MAGIC       ,Serial
// MAGIC       ,Type
// MAGIC       ,RemovedOn
// MAGIC 	  ,count(*)
// MAGIC  FROM RemoteDeviceHistory
// MAGIC  join LeptonUniqueId on ModemSn = Serial
// MAGIC  where RemovedOn is null
// MAGIC  group by RemoteId,Serial,Type,RemovedOn
// MAGIC  having count(*) > 1

// COMMAND ----------

val uniqueidmappingsDF = spark.read.format("delta").load(basePath + "DeltaTable/uniqueidmapping")
val Device_HistoryDF = spark.read.format("delta").load(basePath + "DeltaTable/remotedevicehistory")

// COMMAND ----------

display(Device_HistoryDF.join(uniqueidmappingsDF,$"ModemSn" === $"serial").select("ModemSn","RemoteId", "RemovedOn").count())

// COMMAND ----------

// Load the metric mapping table
val SatRouter = basePath + "DeltaTable/satelliterouter"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS SateliteRouter 
  USING DELTA 
  OPTIONS (path = "$SatRouter")
  """)

// COMMAND ----------

// MAGIC %sql
// MAGIC --SateliteRouter
// MAGIC select distinct model from SateliteRouter --where serialnumber in ('884833','33333281','11111131','6453123','42333234','421333233','77777318','98888321','4929391','423323233')

// COMMAND ----------

// MAGIC %sql select terminalId,SSPCId, timestamp, count(*) from IntelsatusageBronze where Datestamp='2019-12-04' --and hour = '16'  
// MAGIC group by terminalId,SSPCId, timestamp
// MAGIC having count(*) > 1
// MAGIC order by timestamp asc

// COMMAND ----------

// MAGIC %sql select * from IntelsatusageBronze where Datestamp='2019-12-04' --and hour = '16' 
// MAGIC and terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// set fact and aggregation storage locations
val Intelsatusage_silver_step1 = basePath + "DeltaTable/IntelsatUsage-silver-step1"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS IntelsatusageSilverStep1 
  USING DELTA 
  OPTIONS (path = "$Intelsatusage_silver_step1")
  """)


// COMMAND ----------

// MAGIC %sql select terminalId,SSPCId, timestamp, count(*) from IntelsatusageSilverStep1 where Datestamp='2019-12-04' --and hour = '16'  
// MAGIC group by terminalId,SSPCId, timestamp
// MAGIC having count(*) > 1
// MAGIC order by timestamp asc

// COMMAND ----------

// MAGIC %sql select * from IntelsatusageSilverStep1 where Datestamp='2019-12-04' --and hour = '16' 
// MAGIC and terminalId='976563' and SSPCId = '976591' and timestamp='1575417600'

// COMMAND ----------

// set fact and aggregation storage locations
val Intelsatusage_silver_step2 = basePath + "DeltaTable/IntelsatUsage-silver-step2"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS IntelsatusageSilverStep2 
  USING DELTA 
  OPTIONS (path = "$Intelsatusage_silver_step2")
  """)

// COMMAND ----------

// MAGIC %sql select * from IntelsatusageSilverStep2 where Datestamp='2019-12-04'  and terminalId='976563' and SSPCId = '976591' and unix_timestamp='1575417600'

// COMMAND ----------

// set fact and aggregation storage locations
val Intelsatusage_gold_raw = basePath + "DeltaTable/IntelsatUsage-gold-raw"

spark.sql(s"""
CREATE TABLE IF NOT EXISTS IntelsatusageGoldRaw 
  USING DELTA 
  OPTIONS (path = "$Intelsatusage_gold_raw")
  """)

// COMMAND ----------

// MAGIC %sql select * from IntelsatusageGoldRaw where Datestamp='2019-12-04'  and unix_timestamp='1575417600' and Kymeta_metricId in (16,13) --and serialnumber='56812'

// COMMAND ----------

// MAGIC %sql DELETE FROM HubstatsForSilver1

// COMMAND ----------

// MAGIC %sql VACUUM HubstatsForSilver1 RETAIN 180 HOURS

// COMMAND ----------

val r = scala.util.Random
r.nextInt