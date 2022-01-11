// Databricks notebook source
// MAGIC %run ./Configuration

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(sc.parallelize(Array(json)))
  //reader.json(sc.parallelize(Array(json)))
}

// COMMAND ----------

val schema = new StructType().add("data", MapType(StringType, StringType))

val events = jsonToDataFrame("""
{
  "data": {
    "b a": 1,
    "c e": 2
  }
}
""", schema)

display(events.select(explode('data) as (Seq("x", "y"))))


// COMMAND ----------

val asmDataSchema = spark.read.json(basePath + "asm_schema/asm.json").schema
val asmMetaSchema = spark.read.json(basePath + "asm_schema/meta.json").schema
val asmWithMetaSchema = spark.read.json(basePath + "asm_schema/asmwithmeta.json").schema
val schema = new StructType().add("data", MapType(StringType, StringType)).add("meta", MapType(StringType, StringType))

val events = jsonToDataFrame("""
{
 "meta":{"Serial":"TEST_SERIAL_1","Version":"1","Timestamp":"1586982338"}, "data":{"Analog voltage (V)":"3.246","-20V (V)":"-20.573","Source voltage (V)":"14.925","Waveguide Thermistor (V)":"2.047","+1.5V_D (V)":"1.489","Temperature (C)":"29.912","latitude":47.709578333333333,"timestamp-S":1525880289,"longitude":-122.16234666666668,"direction":139.0,"speed":0.25928,"+3.3V_D (V)":"3.268","Gate Voltage (V)":"20.075","gps_fix":3,"altitude":34.9,"Heater voltage (V)":"7.150","Clock voltage (V)":"3.294","Relative Humidity (%)":"22.994","Heater Control (V)":"1.980","+2.5V_D (V)":"2.488","gps_fix_quality":1,"+1.1V_D (V)":"1.099","Board input voltage (V)":"20.200","Gate Current (A)":"0.008","Source current (A)":"0.001","Board input current (A)":"0.500","+5V (V)":"4.989","Heater current (A)":"0.013","rx-lpa":201.4497321421716,"tx-phi":300.0139303023949,"rx-phi":400.0139303023949,"tx-theta":59.667638686288008,"rx-theta":79.667638686288015,"theta-rx":89.0,"theta-tx":88.0,"phi-rx":125.0,"phi-tx":8.0,"lpa-rx":269.0,"lpa-tx":179.0,"antenna-type":"antenna-type-tx","antenna-polarization-type-rx":"LINEAR","antenna-polarization-type-tx":"LINEAR","antenna-frequency-rx":12000000000,"antenna-frequency-tx":14000000000}
}
""", schema)

display(events.select($"meta.Serial", $"meta.Version", $"meta.Timestamp", explode('data) as (Seq("x", "y"))))


//display(events)


// COMMAND ----------

import org.apache.spark.sql.functions.{col, udf}
// Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(Json: String, schema: StructType = null): DataFrame = {
  // SparkSessions are available with Spark 2.0+
  val reader = spark.read
  Option(schema).foreach(reader.schema)
  reader.json(Seq(Json).toDS)
  //reader.json(sc.parallelize(Array(json)))
}

//val ASMjsonToDataFrame = spark.udf.register("ASMjsonToDataFrame",jsonToDataFrame _)


// COMMAND ----------


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val asmDataSchema = spark.read.json(basePath + "asm_schema/asm.json").schema
val asmMetaSchema = spark.read.json(basePath + "asm_schema/meta.json").schema
val asmWithMetaSchema = spark.read.json(basePath + "asm_schema/asmwithmeta.json").schema

val streamingInputDFASM = 
spark.read
  .format("delta")
  .load(basePath + "DeltaTable/ASM-bronze")

 // .withColumn("data", concat(lit("{\"data\":"),$"data",lit("}")))
val InputDFASM = streamingInputDFASM
  .withColumn("data", from_json(col("data"),asmDataSchema))

display(InputDFASM)

// COMMAND ----------

val InputDFASMFaltten = InputDFASM.selectExpr("stack(2, 'data.timestamp-S', data.timestamp-S, 'data.speed', data.speed) as (key, value)")
display(InputDFASMFaltten)

// COMMAND ----------

val streamingJsonDFASM = streamingInputDFASM.select($"Serial",$"Version",$"timestamp",$"Datestamp", $"Hour", from_json(col("data"),schema).alias("data"),$"EnqueuedTime", $"LoadTime")

display(streamingJsonDFASM)

// COMMAND ----------

display(streamingJsonDFASM.select(explode('data) as (Seq("x", "y"))))

// COMMAND ----------

val events = streamingInputDFASM
.withColumn("data", jsonToDataFrame(streamingInputDFASM("data").cast(StringType)) )