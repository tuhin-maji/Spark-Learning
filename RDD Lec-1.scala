// Databricks notebook source
//RDD Creation
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder.appName("CreateRDD").getOrCreate()
val data = Seq(1, 2, 3, 4, 5)
val rdd1 = spark.sparkContext.parallelize(data)
print(rdd1.sum())
 


