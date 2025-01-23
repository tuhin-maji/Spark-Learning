// Databricks notebook source
//RDD Creation
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("CreateRDD").getOrCreate()
val data = Seq(1, 2, 3, 4, 5, 5, 4, 1)
val rdd1 = spark.sparkContext.parallelize(data)
//Distinct 
val unique_rdd = rdd1.distinct()
unique_rdd.collect().foreach(println)

// COMMAND ----------


