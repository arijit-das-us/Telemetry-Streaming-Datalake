// Databricks notebook source
// MAGIC %sql
// MAGIC CREATE TABLE employees (
// MAGIC   employeeId Int,
// MAGIC   DeptId Int,
// MAGIC   EName String)
// MAGIC USING DELTA

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE deplartments (
// MAGIC   DeptId Int,
// MAGIC   DName String)
// MAGIC USING DELTA

// COMMAND ----------

// Create the case classes for our domain
case class Department(DeptId: Int, DName: String)
case class Employee(employeeId: Int, DeptId: Int, EName: String)


// COMMAND ----------



// Create the Departments
val department1 = new Department(123, "Computer Science")
val department2 = new Department(789, "Mechanical Engineering")
val department3 = new Department(345, "Theater and Drama")
val department4 = new Department(901, "Indoor Recreation")

// Create the Employees
val employee1 = new Employee(100000,123,"michael armbrust")
val employee2 = new Employee(120000,789,"xiangrui meng")
val employee3 = new Employee(140000,345,"matei")
val employee4 = new Employee(160000,901,"wendell")
val employee5 = new Employee(80000,567,"michael jackson")

// COMMAND ----------

val Employees = Seq(employee1, employee2, employee3, employee4, employee5)
val dfEmployees = Employees.toDF()
display(dfEmployees)

// COMMAND ----------

val Department = Seq(department1, department2, department3, department4)
val dfDepartment = Department.toDF()
display(dfDepartment)

// COMMAND ----------

//account key set up for azure blob sotrage access
spark.conf.set(
  "fs.azure.account.key.kymetabigdata.blob.core.windows.net",
  "HcmXJ6Ks9sV44ef5JhFtIaAEIgL+EhIF8n1wG/an8wyyVuKe/CbPNrSsQuCN7dKy/f6he4cEBi6JGYuGT6//IA==")

// COMMAND ----------

dfEmployees.write.format("delta").partitionBy("employeeId").save("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/employee")

// COMMAND ----------

dfDepartment.write.format("delta").partitionBy("DeptId").save("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/department")

// COMMAND ----------

val Department1 = Seq(new Department(567, "Electrical Engg"),new Department(456, "Land Engg"))
val dfDepartment1 = Department1.toDF()
display(dfDepartment1)

// COMMAND ----------

dfDepartment1.write.format("delta").partitionBy("DeptId").mode("append").save("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/department")

// COMMAND ----------

val Employees1 = Seq(new Employee(80010,567,"michael Ridd"))
val dfEmployees1 = Employees1.toDF()
dfEmployees1.write.format("delta").partitionBy("employeeId").mode("append").save("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/employee")

// COMMAND ----------

val Employees2 = Seq(new Employee(80020,456,"Den Kogg"))
val dfEmployees2 = Employees2.toDF()
dfEmployees2.write.format("delta").partitionBy("employeeId").mode("append").save("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/employee")

// COMMAND ----------

// Create the case classes for our domain
val department1 = new Department(123, "Computer Science 1")
val department2 = new Department(789, "Mechanical Engineering 2")
val department3 = new Department(345, "Theater and Drama 3")
val department4 = new Department(901, "Indoor Recreation 4")
val department5 = new Department(567, "Electrical Engg 5")
val department6 = new Department(456, "Land Engg 6")
val department7 = new Department(678, "Mobile Engg 7")

// COMMAND ----------

val Department3 = Seq(department1, department2, department3, department4, department5, department6, department7)
val dfDepartment3 = Department3.toDF()
dfDepartment3.write.format("delta").partitionBy("DeptId").mode("overwrite").save("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/department")

// COMMAND ----------

// Create the Employees
val employee1 = new Employee(200000,123,"michael armbrust Jr")
val employee2 = new Employee(220000,789,"xiangrui meng Jr")
val employee3 = new Employee(240000,345,"matei Jr")
val employee4 = new Employee(260000,901,"wendell Jr")
val employee5 = new Employee(82000,567,"michael jackson Jr")
val employee6 = new Employee(83000,678,"michael jackson Jr")

// COMMAND ----------

val Employees = Seq(employee1, employee2, employee3, employee4, employee5, employee6)
val dfEmployees = Employees.toDF()
dfEmployees.write.format("delta").partitionBy("employeeId").mode("append").save("wasbs://kite-int@kymetabigdata.blob.core.windows.net/DeltaTable/Test/employee")