# Databricks notebook source
# MAGIC %run ./Configuration

# COMMAND ----------

#Set basePath according to azure blob storage location
basePath = "wasbs://kite-int@kymetabigdata.blob.core.windows.net/"
streamingInputDFASM = spark.read.format("delta").load(basePath + "DeltaTable/ASM-silver-step1")

asmDataSchema = spark.read.json(basePath + "asm_schema/asm.json").schema
asmMetaSchema = spark.read.json(basePath + "asm_schema/meta.json").schema

streamingJsonDFASM = streamingInputDFASM.select("Serial",from_json(col("data"),asmDataSchema).alias("json_data"))
#.select("Serial","data.`Analog voltage (V)`","data.`-20V (V)`")

# COMMAND ----------

display(streamingInputDFASM)

# COMMAND ----------

from pyspark.sql import *
from functools import partial

#from pyspark.sql import spark, Row


def flatten_table(column_names, column_values):
    row = zip(column_names, column_values)
    _, key = next(row)  # Special casing retrieving the first column
    return [
        Row(Key=key, ColumnName=column, ColumnValue=value)
        for column, value in row
    ]


# COMMAND ----------

streamingInputDFASM.rdd.flatMap(partial(flatten_table, streamingInputDFASM.columns)).toDF().show()