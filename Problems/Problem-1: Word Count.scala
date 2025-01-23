import org.apache.spark.sql.SparkSession
 
val spark = SparkSession.builder
  .appName("Word Count")
  .master("local[*]") // Use local mode for testing
  .getOrCreate()
 
val sc = spark.sparkContext
val data = List("India is a great country", "In India cricket is a famous game", "Sachin plays cricket")
val rdd = sc.parallelize(data)

val words = rdd.flatMap(x=>x.split(" "))
val wordCount = words.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
wordCount.collect().foreach{
  case (word, count)=> println(s"$word->$count")
}
