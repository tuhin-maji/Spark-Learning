// Databricks notebook source
/*

 Example 1:

Input: 
Employee table:
+----+--------+
| id | salary |
+----+--------+
| 1  | 100    |
| 2  | 200    |
| 3  | 300    |
+----+--------+
n = 2
Output: 
+------------------------+
| getNthHighestSalary(2) |
+------------------------+
| 200                    |
+------------------------+

*/
// Define the Employee table data
val employeeData = Seq(
  (1, 100),
  (2, 200),
  (3, 300)
)

// Convert the data to an RDD
val employeeRdd = spark.sparkContext.parallelize(employeeData)

// Convert the RDD to a DataFrame with column names
val employeeDF = employeeRdd.toDF("id", "salary")

// Show the Employee DataFrame
employeeDF.show(false)
import org.apache.spark.sql.functions.{col, desc, dense_rank}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
def getNthHighestSalary(DF: DataFrame, n: Int): Int={
  val w = Window.orderBy(col("salary").desc)
  val rankedDF=DF.withColumn("rank", dense_rank().over(w))
  val nthHighestSalary = rankedDF.where("rank="+n).select("salary").distinct().first().getInt(0)
  nthHighestSalary
}

// COMMAND ----------

val secondHighestSalary = getNthHighestSalary(employeeDF,2)
println(secondHighestSalary)

// COMMAND ----------


