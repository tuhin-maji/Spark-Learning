// Databricks notebook source
import org.apache.spark.sql.types._
/*
Person table:
+----------+----------+-----------+
| personId | lastName | firstName |
+----------+----------+-----------+
| 1        | Wang     | Allen     |
| 2        | Alice    | Bob       |
+----------+----------+-----------+
*/
val personData=Seq((1, "Wang", "Allen"), (2, "Alice", "Bob"))
val personRdd=spark.sparkContext.parallelize(personData) 
val personDF=personRdd.toDF("personId", "lastName", "firstName")
personDF.show(2,false)

// COMMAND ----------

/*
Address table:
+-----------+----------+---------------+------------+
| addressId | personId | city          | state      |
+-----------+----------+---------------+------------+
| 1         | 2        | New York City | New York   |
| 2         | 3        | Leetcode      | California |
+-----------+----------+---------------+------------+
*/
 
val addressData = Seq(
  (1, 2, "New York City", "New York"),
  (2, 3, "Leetcode", "California")
)

// Convert the data to an RDD
val addressRdd = spark.sparkContext.parallelize(addressData)

// Convert the RDD to a DataFrame with column names
val addressDF = addressRdd.toDF("addressId", "personId", "city", "state")

// Show the Address DataFrame
addressDF.show(2, false)

// COMMAND ----------

/*
PROBLEM STATEMENT:
Write a solution to report the first name, last name, city, and state of each person in the Person table. If the address of a personId is not present in the Address table, report null instead.
*/
val personWithAddressDF = personDF.join(addressDF, Seq("personId"),"left").select("firstName", "lastName", "city", "state")
personWithAddressDF.show(10,false)

// COMMAND ----------


