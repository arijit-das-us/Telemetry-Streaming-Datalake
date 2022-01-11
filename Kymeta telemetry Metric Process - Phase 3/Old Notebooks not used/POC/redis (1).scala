// Databricks notebook source
// MAGIC %md
// MAGIC # **Connecting to Redis Database**
// MAGIC In this example, we'll query Redis using its Spark Package Driver.
// MAGIC 
// MAGIC This notebook covers the following:
// MAGIC * Part 1: Set up your Redis Connection
// MAGIC * Part 2: Read & Write Strings to/from Redis
// MAGIC * Part 3: Read & Write Hashes to/from Redis
// MAGIC * Part 4: Read & Write Lists to/from Redis
// MAGIC * Part 5: Read & Write Sets to/from Redis

// COMMAND ----------

// MAGIC %md ##Part 1: Setup your Redis Connection
// MAGIC ###Load the Redis Spark Package and attach Library to cluster
// MAGIC * Redis has a [Spark Package](http://spark-packages.org/package/RedisLabs/spark-redis) that you can download and attach to your cluster

// COMMAND ----------

import com.redislabs.provider.redis._

// COMMAND ----------

// MAGIC %md ###Set Redis Connection Properties

// COMMAND ----------

val redisServerDnsAddress = "kymetacloudservicescache.redis.cache.windows.net"
val redisPortNumber = 6379
val redisPassword = "I02HBd98mbFl7+9SvLC/5wXNzmK0pXWU4gBFKS67KwA="
//val redisConfig = new RedisConfig(new RedisEndpoint("kymetacloudservicescache-int.redis.cache.windows.net:6380,password=C4rIx6CpzlQTuTwuu/nEViQRp0+nytqhyOYgc++RFw0=,ssl=True,abortConnect=False"))
val redisConfig = new RedisConfig(new RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

// COMMAND ----------

// MAGIC %md ##Part 2: Read & Strings to/from Redis

// COMMAND ----------

val stringRDD = sc.parallelize(Seq(("key1", "value1"), ("key2", "value2")))

// COMMAND ----------

sc.toRedisKV(stringRDD)(redisConfig)

// COMMAND ----------

//The following example overrides the default setting of 3 partitions in the RDD with a new value of 5.
//Each partition consists of a set of Redis cluster hashslots contain the matched key names.
val keysRDD = sc.fromRedisKeyPattern("key*", 5)(redisConfig)

val stringRDD = keysRDD.getKV

// COMMAND ----------

stringRDD.collect()

// COMMAND ----------

// MAGIC %md ##Part 3: Read & Write Hashes to/from Redis

// COMMAND ----------

val stringHashRDD = sc.parallelize(Seq(("field1", "value1"), ("field2", "value2")))

sc.toRedisHASH(stringHashRDD, "hashkey1databrick")(redisConfig)

// COMMAND ----------

val keysRDD = sc.fromRedisKeyPattern("hashkey*")(redisConfig)

val hashRDD = keysRDD.getHash

// COMMAND ----------

hashRDD.collect()

// COMMAND ----------

// MAGIC %md ##Part 4: Read & Write Lists to/from Redis

// COMMAND ----------

val stringListRDD = sc.parallelize(Seq("item1", "item2", "item3"))

sc.toRedisLIST(stringListRDD, "listkey1")(redisConfig)

// COMMAND ----------

val keysRDD = sc.fromRedisKeyPattern("listkey*")(redisConfig)

val listRDD = keysRDD.getList

// COMMAND ----------

listRDD.collect()

// COMMAND ----------

// MAGIC %md ##Part 5: Read & Write Sets to/from Redis
// MAGIC * Note the repeating value in the RDD before writing to Redis
// MAGIC * Only the distinct set of values is persisted

// COMMAND ----------

val stringSetRDD = sc.parallelize(Seq("member1", "member2", "member3", "member3"))

sc.toRedisSET(stringSetRDD, "setkey1")(redisConfig)

// COMMAND ----------

val keysRDD = sc.fromRedisKeyPattern("setkey*")(redisConfig)

val setRDD = keysRDD.getSet

// COMMAND ----------

setRDD.collect()