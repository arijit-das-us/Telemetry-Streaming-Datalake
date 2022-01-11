// Databricks notebook source
//account key set up for azure blob sotrage access
spark.conf.set(
  "fs.azure.account.key.kymetabigdata.blob.core.windows.net",
  "HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==")

// COMMAND ----------

val factEmp = spark.readStream
  .format("delta")
  .load("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/employee")

// COMMAND ----------

display(factEmp)

// COMMAND ----------

val factDept = spark.read
  .format("delta")
  .load("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/department")

// COMMAND ----------

display(factDept)

// COMMAND ----------

val EmpWithDept = factEmp.join(factDept).where(factEmp("DeptId") === factDept("DeptId"))

// COMMAND ----------

display(EmpWithDept)