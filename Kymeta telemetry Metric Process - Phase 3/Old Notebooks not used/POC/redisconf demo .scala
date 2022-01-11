// Databricks notebook source
// DBTITLE 1,RedisConf 2016 Databricks Demo
// MAGIC %md 
// MAGIC 
// MAGIC 
// MAGIC 
// MAGIC <img src="https://spark-summit.org/east-2015/wp-content/uploads/sites/9/2014/10/databricks_logoTM_1200px.png" alt="Databricks" style="width: 200px;"/> + ![Spark](https://databricks.com/wp-content/themes/databricks/assets/images/spark/spark_logo.png?v=2.75)  + ![redisconf](http://redisconference.com/wp-content/uploads/2016/03/logo.png) 

// COMMAND ----------

// MAGIC %md 
// MAGIC  - Using Redis with SparkSQL
// MAGIC  - Using Redis with Spark Dataframes for Text Analysis
// MAGIC  - Using Redis to Serve Machine Learning Models

// COMMAND ----------

// MAGIC %md ## Set Redis Connection Properties
// MAGIC Add the following to Spark Config when starting up a cluster:
// MAGIC 
// MAGIC * redis.host pub-redis-11995.us-west-2-1.1.ec2.garantiadata.com
// MAGIC * redis.port 11995
// MAGIC * redis.auth XXXXXXX

// COMMAND ----------

import com.redislabs.provider.redis._

// COMMAND ----------

display(table("amazon"))

// COMMAND ----------

// MAGIC %md ## Using Redis with Spark SQL
// MAGIC 
// MAGIC <img src="https://docs.google.com/drawings/d/1LcV4_8Hkaf_LwY78X1KkuRiKkp-HVmodsZ4QjD02HYU/pub?w=960&amp;h=720">

// COMMAND ----------

// MAGIC %md Create a Pair RDD
// MAGIC * Use the Amazon Standard Identification Number (ASIN) as the Key
// MAGIC * Store Brands and Items

// COMMAND ----------

val brandRdd = table("amazon").select("asin", "brand" ).distinct.na.drop().map{x =>
  (x.getString(0), x.getString(1))
}
sc.toRedisHASH(brandRdd, "brand")

// COMMAND ----------

val itemRDD = table("amazon").select("asin", "title" ).distinct.na.drop().map{x =>
  (x.getString(0), x.getString(1))
}
sc.toRedisHASH(itemRDD, "item")

// COMMAND ----------

// MAGIC %md In Redis
// MAGIC  - HGET brand B000HCR8C4
// MAGIC  - HGET item B000HCR8C4

// COMMAND ----------

val items = sc.fromRedisHash("item*").toDF("asin", "title")
val brands = sc.fromRedisHash("brand*").toDF("asin", "brand")

// COMMAND ----------

items.registerTempTable("items")
brands.registerTempTable("brands")

// COMMAND ----------

// MAGIC %sql select * from items inner join brands on items.asin = brands.asin

// COMMAND ----------

// MAGIC %sql select brand, count(1) from items inner join brands on items.asin = brands.asin
// MAGIC group by brand order by count(1) desc limit 10

// COMMAND ----------

// MAGIC %md ##Using Redis with Spark Dataframes for Text Analysis

// COMMAND ----------

val itemBrands = sql("select items.asin, title, brand from items inner join brands on items.asin = brands.asin")

// COMMAND ----------

itemBrands.cache().take(100)

// COMMAND ----------

import org.apache.spark.ml.feature._
import org.apache.spark.ml._


val tokenizer = new Tokenizer().setInputCol("title").setOutputCol("keywords")

// COMMAND ----------

val keywords = tokenizer.transform(itemBrands)

// COMMAND ----------

display(keywords)

// COMMAND ----------

import org.apache.spark.sql.functions._
val keywordsByBrand = keywords.select(explode($"keywords").as("keyword"), $"brand", lit(1).as("count")).groupBy("brand", "keyword").agg(sum($"count"))

// COMMAND ----------

display(keywordsByBrand.filter("brand like '%Disney%'").orderBy(desc("sum(count)")).limit(10))

// COMMAND ----------

display(keywordsByBrand.filter("brand like '%Apple%'").orderBy(desc("sum(count)")))

// COMMAND ----------

// MAGIC %md ## Using Redis to Serve Machine Learning Models

// COMMAND ----------

// MAGIC %md 
// MAGIC ![Amazon Recommendations](https://blog.kissmetrics.com/wp-content/uploads/2011/04/amazon-email1.jpg)

// COMMAND ----------

// MAGIC %sql select * from amazon

// COMMAND ----------

// MAGIC %md ![Alternating Least Squares - Matrix Factorization](https://raw.githubusercontent.com/cfregly/spark-after-dark/master/img/ALS.png)

// COMMAND ----------

// MAGIC %md ![Example](http://i.imgur.com/tTFPMKe.png)

// COMMAND ----------

val dataset = table("amazon")
display(dataset.filter("user = 'A3OXHLG6DIBRW8'").select("brand", "title", "rating").orderBy(desc("rating")))

// COMMAND ----------

import org.apache.spark.ml.recommendation._

val dataset = table("amazon")
val hashId = sqlContext.udf.register("generateHashCode", (s : String) => s.hashCode)
val trainingData = dataset.withColumn("itemId", hashId($"asin")).withColumn("userId", hashId($"user"))
val als = new ALS().setItemCol("itemId").setUserCol("userId")
val model = als.fit(trainingData)

// COMMAND ----------

val userItems = itemBrands
  .withColumn("user", lit("A3OXHLG6DIBRW8"))
  .withColumn("itemId", hashId($"asin"))
  .withColumn("userId", hashId($"user"))

// COMMAND ----------

val recommendations = model.transform(userItems).cache()

// COMMAND ----------

display(recommendations.select("prediction", "asin", "title", "brand").orderBy(desc("prediction")))

// COMMAND ----------

val kvRecommendations = recommendations.selectExpr("asin", "string(prediction) as prediction").na.drop.map(x => (x.getString(0), x.getString(1)))
sc.toRedisZSET(kvRecommendations, "recommendation")

// COMMAND ----------

// MAGIC %md Run in console
// MAGIC - ZCARD recommendation
// MAGIC - ZSCORE recommendation asin
// MAGIC - ZREVRANGE recommendation 0 10 WITHSCORES

// COMMAND ----------

// MAGIC %sql select * from items where title like '%iPhone%'

// COMMAND ----------

// MAGIC %sql select * from items where title like '%Blackberry%'

// COMMAND ----------

// MAGIC %md What does this person want?

// COMMAND ----------

// MAGIC %md HGET item B009EFVSEO 

// COMMAND ----------

display(itemBrands.join(recommendations, "asin").orderBy(desc("prediction")).limit(10))

// COMMAND ----------

// MAGIC %md #Lesson Learned
// MAGIC 
// MAGIC ![Meme](https://i.imgflip.com/13xyz4.jpg)