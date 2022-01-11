// Databricks notebook source
displayHTML("Configuration running...")

// COMMAND ----------

//Configuration parameters

val stage = "debug"

// The Kafka broker hosts and topic used to write to Kafka
val kafkaBrokers="wn0-kym-ka.tg12ghcypikufl1gl5tbxsofsd.xx.internal.cloudapp.net:9092,wn1-kym-ka.tg12ghcypikufl1gl5tbxsofsd.xx.internal.cloudapp.net:9092"
val SourcekafkaTopic="intelsatusage"
val kafkaTopicToPublish = "intelsatusage-Published"
println("Finished setting Kafka broker and topic configuration for tpoic hubstats.")

//account key set up for azure blob sotrage access
spark.conf.set(
  "fs.azure.account.key.kymetabigdata.blob.core.windows.net",
  "HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==")


spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass",
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")


// Set basePath according to azure blob storage location
val basePath = "wasbs://kite-prod@kymetabigdata.blob.core.windows.net/"

//redis configuration
val redisServerDnsAddress = "kymetacloudservicescache.redis.cache.windows.net"
val redisPortNumber = 6379
val redisPassword = "I02HBd98mbFl7+9SvLC/5wXNzmK0pXWU4gBFKS67KwA="

// Blob configuration
val blobAccount = "dwdatabricksblob"
val blobContainer = "dwstor"

// Cosmos DB configuration
val cdbAccountName = "dw-cdb-acc"
val cdbDatabase = "dw-cdb-db"
val cdbCollection = "dw-cdb-col"
val cdbEndpoint = "https://"+cdbAccountName+".documents.azure.com:443/"

// Event hub configuration
val eventHubNamespace = "dwevhub-ns"
val eventHubName = "dw-evhub"


// Azure SQL DW configuration
// Requires Azure Blob storage to transfer data efficiently between an Azure Databricks cluster and a SQL DW instance.
// See - https://docs.azuredatabricks.net/spark/latest/data-sources/azure/sql-data-warehouse.html
// Please create a blob storage account and container, then configure the details below
val JDBC_URL_DATA_WAREHOUSE = "jdbc:sqlserver://kymetaconnected.database.windows.net:1433;database=kymeta-bigdata;user=kcadmin@kymetaconnected;password=$Curious0celot;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val POLYBASE_TEMP_DIR = "wasbs://kite-int@kymetabigdata.blob.core.windows.net/tempdir"

// SQL DB configuration
val sqldbsrvname = "dw-sqldb-srv"
val sqldbdatabase = "dw-demo-sqldb"
val sqlsrvuser = "saadmin"
val sqlsrvpassword = "password"


//setting shared key for API call
val name = "sharedKey"
val value = "MIICWgIBAAKBgGMmxEtt6wqRvQZiV9zhTCqi6f8v7Cvtl7SXh05eal3jEB1+TVTM1zv6I0C0Hmzrp1EV35WgPYq3B+4BzbGOEma+Kusx/uSzbqxrIZucoTsRxRWxC2m8z0vCIxrsnOVwqlhlQYQNgpT8dhP+eA+cnq+ErHpriXSmQfmXL1h9oC9zAgMBAAECgYA4u4bXzhYN3yP0UjMJ/JOzVbJkRIxy+iiyuX0/N0DPZcvjxFAFNIv3EoI3VJiQJCqBd+2N1LlB9MaBxeBnNu6JylSVCrpH1Gj8YHPk2RnrPW2j52IvzpsIJhcQUWZfZoaYXLNIBUX2eCvyDyZNcJm6hcB9wNfpbwInMYjsFwQpwQJBAK0dYu9YDjKgPAmtd9CzGxKR0ZuFJi5JcqCZajvba14eJuzCdZyS0Y83T1eawvHlGB8uOhJkKPBebILtUn0PCpMCQQCSn7hI/dzeLbBKZ4WSHXzB+ldnZfN8bQxDZcc3RGyK1jQxTTHsX3kN241QDwByH1aBBNiXbCgTLHserMVG+/OhAkBNqCwUgCTGUxj7omRoK6BOYTltEXrCMtKH9qowNcrhSpddiBIobbgyDba67sLarlF2007bpzCyOzlkNj/Vt+SfAkAuUyxEU4DO6ZyDRYXcDlj2aIPo9Tsimsl/Gc8BVSr+CoNe+EbHqbpzeGSDYNoBNIl+JevQm6lltW4I2QlTThNBAkAkXmc/y5S7I9H8+yHDD36UXTwHoSz5D6RL5xlYEHxKFisAAnevj7onaiTC2Bx26Zc3bhfUZ8L0vLVpixl+CuWv"

//setting url for API calls
val metricmappingsURL = "http://kymetacloudservicesremotemetrics-INTEGRATION.azurewebsites.net/v2.0/config/metricmappings"
val metricprovidersURL = "http://kymetacloudservicesremotemetrics-INTEGRATION.azurewebsites.net/v2.0/config/metricproviders"
val metricsURL = "http://kymetacloudservicesremotemetrics-INTEGRATION.azurewebsites.net/v2.0/config/metrics"

// Override the default number of partitions created during shuffle
spark.conf.get("spark.sql.shuffle.partitions")
print(sc.defaultParallelism)
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

displayHTML("Configuration compete with default partitions of " + sc.defaultParallelism )

// COMMAND ----------

spark.conf.get( "spark.sql.streaming.stateStore.providerClass")