// Databricks notebook source
// MAGIC %md 
// MAGIC #Process Metrics - Step 04b
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
val customRdd = table("MetricCustomJoinGold")
.select("unixTimestamp", "remoteId", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId")
.orderBy("remoteId","unixTimestamp")
.map{x =>
  (x.getLong(0), x.getString(1), x.getString(2), x.getLong(3), x.getLong(4), x.getLong(5))
}

// COMMAND ----------

//display(customRdd)

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

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

import scala.util.parsing.json.JSONObject
import scala.collection.mutable.HashMap


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
     strReturn = JSONObject(strMap.toMap).toString()
    } else if (strMap!=null && strMap.size == 1){
      val keysRDD = sc.fromRedisKeyPattern("DBcustomJoin" + remoteId + "*")(redisConfig)
      //val hashRDD = keysRDD.getHash
      val hashArrayMap = keysRDD.getHash.collect().toMap
      val newhashArrayMap = hashArrayMap.size match {
        case 0 => Map("130" -> "0", "131" -> "0")
        case _ => hashArrayMap
        }
      val newStrMap = newhashArrayMap ++ strMap.toMap
      strReturn = JSONObject(newStrMap.toMap).toString()
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

display(df.orderBy("remoteId","unixTimestamp"))

// COMMAND ----------

import scala.util.parsing.json.JSONObject
import scala.collection.mutable.HashMap

var hashMap = new HashMap[String, Map[String,String]]()
var x = new java.util.HashMap[String,String]() 
val stringHashRDD = sc.parallelize(Seq(("field1", "value1"), ("field2", "value2")))

//sc.toRedisHASH(sc.parallelize(Seq(("field1", "value1"), ("field2", "value2"))), "databricktest")(redisConfig)

def k_function(str:String,remoteId:String): String = {
  val strMap = 
  str.substring(1, str.length - 2)
        .split("],")
        .map(_.split(","))
        .map { case Array(k, v) => (k.trim()substring(1), v.trim())}
        .toMap
    if(strMap!=null && strMap.size == 1){
    //  sc.toRedisHASH(sc.parallelize(Seq(("field1", "value1"), ("field2", "value2"))), "databricktest")(redisConfig)
      //if(strMap("130") == null)
       //  strMap("130") = "0"
      //if(strMap("131") == null)
      //    strMap("131") = "0"
     // hashMap(remoteId)=strMap
     // println(hashMap.size)
     //sc.toRedisHASH(sc.parallelize(Seq(("field1", "value1"), ("field2", "value2"))), "databrick" + remoteId)(redisConfig)
    //toRedisHASH(stringHashRDD, "databrick" + remoteId)(redisConfig)
    }
  
    JSONObject(strMap).toString()
}


val newRdd = brandRdd.map( row => {
        val unixTimestamp = row._1
        //val feature = row._2.map(_.toDouble)
        //val QD = k_function(feature)
        val remoteId = row._2
        val valueInString = k_function(row._3,remoteId)
        x.put(remoteId,valueInString)
        println(x)
        //println (remoteId) 
        //sc.toRedisHASH(sc.parallelize(Seq(("field1", "value1"), ("field2", "value2"))), "databrick")(redisConfig)
        (unixTimestamp,remoteId,valueInString)
    })

newRdd.take(10).foreach(println)
println(x.size)

// COMMAND ----------

val dfcustomMetricJOIN = streamingInputDF.join(factCustommetrics).where(streamingInputDF("kymetaMetricId") === factCustommetrics("mappingIds") && ( factCustommetrics("mappingType") === "JOIN")) 
.select("unixTimestamp","dateStamp", "valueInString", "kymetaMetricId", "metricProviderId", "categoryId", "RemoteId","metricId","mappingType")
.withColumn("TimestampYYMMddHHmmss", from_unixtime($"unixTimestamp","yyyy-MM-dd'T'HH:mm:ss") cast TimestampType)


// COMMAND ----------

val dfCustomMetricStreamJOIN = dfCustomMetricsAggJoin
.withColumn("valueInDouble", lit(null).cast(DecimalType(30,15)))
.withColumn("elementId", lit(null).cast(StringType))
.withColumn("metric", lit(null).cast(StringType))
.withColumnRenamed("metricId","kymetaMetricId")