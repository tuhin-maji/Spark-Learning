// Databricks notebook source
/*
Example 1:

Input: 
Employee table:
+----+-------+--------+-----------+
| id | name  | salary | managerId |
+----+-------+--------+-----------+
| 1  | Joe   | 70000  | 3         |
| 2  | Henry | 80000  | 4         |
| 3  | Sam   | 60000  | Null      |
| 4  | Max   | 90000  | Null      |
+----+-------+--------+-----------+
Output: 
+----------+
| Employee |
+----------+
| Joe      |
+----------+
Write a solution to find the employees who earn more than their managers.
*/


// COMMAND ----------

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
 
// Create an RDD with employee data
val employeeRDD = spark.sparkContext.parallelize(Seq(
  Row(1, "Joe", 70000, 3),
  Row(2, "Henry", 80000, 4),
  Row(3, "Sam", 60000, null),
  Row(4, "Max", 90000, null)
))

// Define the schema for the DataFrame
val schema = StructType(Array(
  StructField("id", IntegerType, nullable = false),
  StructField("name", StringType, nullable = false),
  StructField("salary", IntegerType, nullable = false),
  StructField("managerId", IntegerType, nullable = true)
))

// Create a DataFrame from the RDD with the schema
val employeeDF = spark.createDataFrame(employeeRDD, schema)

// Show the DataFrame
employeeDF.show()


// COMMAND ----------

val employeeDF1=employeeDF.as("e1").join(employeeDF.as("e2"),col("e1.managerId")===col("e2.id"),"left")
.select(col("e1.id"),col("e1.name"),col("e1.salary"),col("e2.salary").as("managerSalary"))
.where("salary>managerSalary")
.select(col("name").as("Employee"))
employeeDF1.show(10,false)
