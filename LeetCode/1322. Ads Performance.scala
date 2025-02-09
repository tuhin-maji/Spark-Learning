// Databricks notebook source
/*
Write an pyspark code to find the ctr of each Ad.Round ctr to 2 
decimal points. Order the result table by ctr in descending order 
and by ad_id in ascending order in case of a tie. 
Ctr=Clicked/(Clicked+Viewed) 

INPUT 
+-----+-------+-------+
|AD_ID|USER_ID| ACTION|
+-----+-------+-------+
|    1|      1|Clicked|
|    2|      2|Clicked|
|    3|      3| Viewed|
|    5|      5|Ignored|
|    1|      7|Ignored|
|    2|      7| Viewed|
|    3|      5|Clicked|
|    1|      4| Viewed|
|    2|     11| Viewed|
|    1|      2|Clicked|
+-----+-------+-------+
OUTPUT 
+-----+----+
|AD_ID|CTR |
+-----+----+
|1    |0.67|
|3    |0.5 |
|2    |0.33|
|5    |0.0 |
+-----+----+

 
*/

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
 
val schema = StructType(Seq(
  StructField("AD_ID", IntegerType, nullable = false),
  StructField("USER_ID", IntegerType, nullable = false),
  StructField("ACTION", StringType, nullable = false)
))

// Create data
val data = Seq(
  (1, 1, "Clicked"),
  (2, 2, "Clicked"),
  (3, 3, "Viewed"),
  (5, 5, "Ignored"),
  (1, 7, "Ignored"),
  (2, 7, "Viewed"),
  (3, 5, "Clicked"),
  (1, 4, "Viewed"),
  (2, 11, "Viewed"),
  (1, 2, "Clicked")
)

// Convert sequence to DataFrame
val df = spark.createDataFrame(data).toDF("AD_ID", "USER_ID", "ACTION")

// Show DataFrame
df.show()


// COMMAND ----------

import org.apache.spark.sql.functions._
val cliked_df = df.where("ACTION='Clicked'").groupBy("AD_ID").agg(countDistinct(col("USER_ID")).as("click_count"))
 
cliked_df.show(10,false)

// COMMAND ----------

val cliked_viewed_df = df.where("ACTION in ('Clicked','Viewed')").groupBy("AD_ID").agg(countDistinct(col("USER_ID")).as("click_count"))
 
cliked_df.show(10,false)

// COMMAND ----------

val click_view_df= df.groupBy("AD_ID").agg(sum(when(col("ACTION")==="Clicked", 1).otherwise(0)).as("click_count"),
sum(when(col("ACTION")==="Viewed", 1).otherwise(0)).as("view_count")
).withColumn("CTR",round(col("click_count")*1.0/(col("click_count")+col("view_count")),2))
.withColumn("CTR",coalesce(col("CTR"),lit(0.0)))
.select("AD_ID","CTR").orderBy(col("CTR").desc,col("AD_ID").asc)

// COMMAND ----------

click_view_df.show(10,false)
