// Databricks notebook source
/*
Input: 
Customers table:
+----+-------+
| id | name  |
+----+-------+
| 1  | Joe   |
| 2  | Henry |
| 3  | Sam   |
| 4  | Max   |
+----+-------+
Orders table:
+----+------------+
| id | customerId |
+----+------------+
| 1  | 3          |
| 2  | 1          |
+----+------------+
Output: 
+-----------+
| Customers |
+-----------+
| Henry     |
| Max       |
+-----------+

Write a solution to find all customers who never order anything.

Return the result table in any order.
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

 
// Define the data for the Name table
val customersData = Seq(
  (1, "Joe"),
  (2, "Henry"),
  (3, "Sam"),
  (4, "Max")
)

// Create DataFrame from the sequence of data
val customersRdd=spark.sparkContext.parallelize(customersData)
val customersDF = customersRdd.toDF("id", "name")

// Show the DataFrame
customersDF.show()


// COMMAND ----------

 
// Define the data for the Orders table
val ordersData = Seq(
  (1, 3),
  (2, 1)
)

// Create DataFrame from the sequence of data
val ordersRdd = spark.sparkContext.parallelize(ordersData)
val ordersDF = ordersRdd.toDF("id", "customerId")

// Show the DataFrame
ordersDF.show()


// COMMAND ----------

customersDF.printSchema

// COMMAND ----------

val customersWithOrders = customersDF.join(ordersDF,customersDF("id")===ordersDF("customerId"),"left")
val neverOrderedDF = customersWithOrders.where("customerId is null").select("name")
neverOrderedDF.show(10,false)

// COMMAND ----------


