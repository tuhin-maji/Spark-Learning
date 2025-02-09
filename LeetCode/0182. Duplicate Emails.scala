// Databricks notebook source
/*
Example 1:

Input: 
Person table:
+----+---------+
| id | email   |
+----+---------+
| 1  | a@b.com |
| 2  | c@d.com |
| 3  | a@b.com |
+----+---------+
Output: 
+---------+
| Email   |
+---------+
| a@b.com |
+---------+
Write a solution to report all the duplicate emails. Note that it's guaranteed that the email field is not NULL.

Return the result table in any order.
*/


// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
 
// Define the data for the Person table
val personData = Seq(
  (1, "a@b.com"),
  (2, "c@d.com"),
  (3, "a@b.com")
)

// Create DataFrame from the sequence of data
val personRDD = spark.sparkContext.parallelize(personData)
val personDF = personData.toDF("id", "email")

// Show the DataFrame
personDF.show()


// COMMAND ----------

val duplicateEmail = personDF.groupBy("email").agg(count("id").as("count")).where("count>1").select(col("email").as("Email"))
duplicateEmail.show(10,false)
