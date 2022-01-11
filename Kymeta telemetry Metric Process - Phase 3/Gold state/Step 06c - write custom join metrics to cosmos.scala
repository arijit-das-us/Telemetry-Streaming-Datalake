// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Step 06c
// MAGIC 1. read messages from Kafka topic intelsatusage
// MAGIC 2. get the NetworkProfile->ID and join with sspc table by rowkey to get the usage Name
// MAGIC 3. get the terminal ID and join terminal table by rowkey to get sat router ID
// MAGIC 4. join router table by rowkey and sat router ID
// MAGIC 5. get the model serial number
// MAGIC 6. get remote ID from model history table based on modem serial number and timestamp
// MAGIC 7. join metricmapping table by primaryRawSymbol and mappingType by the usage->byteReceived and byteTransmitted and usage type (USAGE_NMS, USAGE_MGMT, USAGE_DATA)
// MAGIC 8. get the Kymeta metric Id from above join from metric mapping table
// MAGIC 9. stage the data (remote ID, Metric ID, provider ID, Timestamp. value) to delta table

// COMMAND ----------

// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

val customRdd = table("MetricCustomJoinGold")
.select("unixTimestamp", "remoteId", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId")
.filter($"unixTimestamp" > (unix_timestamp() - lit(3600)))
//.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)
//.filter($"TimestampYYMMddHHmmss" >= (date_sub(current_timestamp(), 1)))
.orderBy("remoteId","unixTimestamp")
.map{x =>
  (x.getLong(0), x.getString(1), x.getString(2), x.getLong(3), x.getLong(4), x.getLong(5))
}

// COMMAND ----------



val schema = StructType(Array(
    StructField("unixTimestamp", LongType, nullable = true),
    StructField("valueInString", StringType, nullable = true),
    StructField("kymetaMetricId", LongType, nullable = true),
    StructField("metricProviderId", LongType, nullable = true),
    StructField("categoryId", LongType, nullable = true),
    StructField("remoteId", StringType, nullable = true)
    ))

// COMMAND ----------

import com.redislabs.provider.redis._

val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

// COMMAND ----------

//import scala.util.parsing.json.JSONObject
//import scala.collection.mutable.HashMap

val sc = spark.sparkContext
var dataRDD = sc.emptyRDD[Row]
val sparkSession = SparkSession.builder().enableHiveSupport().getOrCreate()
var df=sparkSession.createDataFrame(dataRDD, schema)

def k_function(str:String,remoteId:String): String = {
  var strReturn = ""
  val strMap = 
  str.substring(1, str.length - 2)
        .split("],")
        .map(_.split(","))
        .map { case Array(k, v) => (k.trim()substring(1), v.trim())}
        
    if(strMap!=null && strMap.size > 1){     
     sc.toRedisHASH(sc.parallelize(strMap), "DBcustomJoin" + remoteId)(redisConfig)
      val newStrMap = strMap.toMap
      strReturn = newStrMap("130") + "," + newStrMap("131")
     //strReturn = JSONObject(strMap.toMap).toString()
    } else if (strMap!=null && strMap.size == 1){
      val keysRDD = sc.fromRedisKeyPattern("DBcustomJoin" + remoteId + "*")(redisConfig)
      //val hashRDD = keysRDD.getHash
      val hashArrayMap = keysRDD.getHash.collect().toMap
      val newhashArrayMap = hashArrayMap.size match {
        case 0 => Map("130" -> "0", "131" -> "0")
        case _ => hashArrayMap
        }
      val newStrMap = newhashArrayMap ++ strMap.toMap
      strReturn = newStrMap("130") + "," + newStrMap("131")
      //strReturn = JSONObject(newStrMap.toMap).toString()
      //print(JSONObject(strMap.toMap).toString())
    }
  strReturn
    
}

customRdd.collect().foreach(a =>{

    val unixTimestamp = a._1
    val remoteId = a._2
    val strValue = a._3
    val kymetaMetricId = a._4
    val metricProviderId = a._5
    val categoryId = a._6
    val strValueJson = k_function(strValue,remoteId)

    val rowValues = List(unixTimestamp, strValueJson, kymetaMetricId, metricProviderId, categoryId, remoteId)
    
    // Create `Row` from `Seq`
    val row = Row.fromSeq(rowValues)

    // Create `RDD` from `Row`
    val rdd = sc.makeRDD(List(row))

    // Create `DataFrame`
    val newRow = sqlContext.createDataFrame(rdd, schema)

    df = df.union(newRow)  
}                        
)


// COMMAND ----------

val dfCustomMetricStreamJOIN = df
.withColumn("valueInDouble", lit(null).cast(DecimalType(30,15)))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))

// COMMAND ----------

import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.CosmosDBSpark
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming.CosmosDBSinkProvider
val configMapCustomMetricsJoin = Map(
  "Endpoint" -> cdbEndpoint,
  "Masterkey" -> cdbMasterkey,
  "Database" -> cdbDatabaseRaw,
  "Collection" -> cdbCollectionLiveRaw,
  "preferredRegions" -> cdbRegions,
  "Upsert" -> "true",
  "WritingBatchSize" -> cdbBatchSize)
val configCustomMetricsJoin = Config(configMapCustomMetricsJoin)

// COMMAND ----------

val dfcustomMetricToCosmos = dfCustomMetricStreamJOIN
.select("elementId", "unixTimestamp","metric", "valueInDouble", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "remoteId")
.withColumn("id",concat(col("remoteId"),lit('|'), col("unixTimestamp"),lit('|'), col("kymetaMetricId"),lit('|'),col("metricProviderId"),lit('|'),col("categoryId")))
.coalesce(32)

// COMMAND ----------

dfcustomMetricToCosmos.write.mode(SaveMode.Overwrite).cosmosDB(configCustomMetricsJoin)