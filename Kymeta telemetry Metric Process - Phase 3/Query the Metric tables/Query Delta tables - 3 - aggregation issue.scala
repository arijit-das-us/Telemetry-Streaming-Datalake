// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

// MAGIC %sql
// MAGIC select element, unix_timestamp, Datestamp, metric, value, serialnumber, model, deviceType, Kymeta_metricId, metricProviderId from MetricSilver2 where 
// MAGIC unix_timestamp >= 1614556800 and unix_timestamp < 1617235200 and 
// MAGIC metricProviderId = 3 and serialnumber = '69009'
// MAGIC order by unix_timestamp desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from MetricGold where metricProviderId = 3 
// MAGIC and kymetaMetricId = 9003
// MAGIC --and dateStamp >= '2021-06-01' and dateStamp <= '2021-06-30'
// MAGIC and unixTimestamp >= 1625097600 and unixTimestamp < 1627160400
// MAGIC --select * from MetricGold where metricProviderId = 2 and kymetaMetricId in (71,72,84,85) and unixTimestamp > 1598918400
// MAGIC and remoteId = 'b8862f82-5500-4640-85a3-f92b101c4901'
// MAGIC order by unixTimestamp asc

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT unixTimestamp, kymetaMetricId, remoteId, COUNT(*)
// MAGIC FROM MetricGold where metricProviderId = 3 
// MAGIC and unixTimestamp >= 1614556800 and unixTimestamp < 1617235200
// MAGIC GROUP BY unixTimestamp, kymetaMetricId, remoteId
// MAGIC HAVING COUNT(*) > 1

// COMMAND ----------



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

// MAGIC %sql
// MAGIC select * from MetricAggregatedByDay where unixTimestamp >= 1601510400 and kymetaMetricId = 44
// MAGIC order by unixTimestamp, remoteId

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
// MAGIC select  unix_timestamp(substring(dateStamp,0,7),'yyyy-MM') as UnixMonth, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where --unixTimestamp >= 1601510400 --and 
// MAGIC kymetaMetricId = 9003
// MAGIC group by unix_timestamp(substring(dateStamp,0,7),'yyyy-MM'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0 and UnixMonth = 1601510400)
// MAGIC order by UnixMonth, remoteId, metricProviderId, kymetaMetricId

// COMMAND ----------

// MAGIC %sql
// MAGIC select  unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd HH'),'yyyy-MM-dd HH') as UnixHour, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where dateStamp = '2020-10-15' and unixTimestamp >= 1602802800 and unixTimestamp < 1602082800 --and kymetaMetricId = 44
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd HH'),'yyyy-MM-dd HH'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0)
// MAGIC order by UnixHour desc, remoteId, metricProviderId, kymetaMetricId

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
// MAGIC ModemSn = '17385' order by NetModemId

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from RemoteDeviceHistory
// MAGIC where Type = 'DEV_SIM' and RemovedOn is null
// MAGIC order by Serial
// MAGIC --where Type = 'DEV_CLRTR'
// MAGIC --where Serial = '54925'

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
// MAGIC select count(distinct remoteId) from MetricAggregatedByMonth where metricProviderId = 6

// COMMAND ----------

// MAGIC %sql
// MAGIC select unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd') as UnixDay, remoteId, kymetaMetricId, metricProviderId, categoryId, sum(valueInDouble) from MetricGold where 
// MAGIC unixTimestamp >= 1616284800 and unixTimestamp < 1618012800 and
// MAGIC remoteId = 'b3ddcf7c-5940-4063-8cc9-5a6ab58f7a8a' and metricProviderId = 3
// MAGIC group by unix_timestamp(from_unixtime(unixTimestamp,'yyyy-MM-dd'),'yyyy-MM-dd'), remoteId, kymetaMetricId, metricProviderId, categoryId
// MAGIC having(sum(valueInDouble) != 0)-- and UnixDay = 1601942400)
// MAGIC order by UnixDay, remoteId, metricProviderId, kymetaMetricId