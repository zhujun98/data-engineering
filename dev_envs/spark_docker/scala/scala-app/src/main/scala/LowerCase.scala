import org.apache.spark.sql.SparkSession


object LowerCase extends App {
  val spark = SparkSession.builder().appName("spark_test").getOrCreate()

  val logOfSongs = Seq(
    "Despacito",
    "Nice for what",
    "No tears left to cry",
    "Despacito",
    "Havana",
    "In my feelings",
    "Nice for what",
    "despacito",
    "All the stars"
  )

  val rdd = spark.sparkContext.parallelize(logOfSongs)
  rdd.map(x => x.toLowerCase).collect().foreach(println)

  spark.stop()
}
