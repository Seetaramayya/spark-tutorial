package learn.spark.basics

import org.apache.spark.sql.SparkSession

object ReadMovies extends App {
  private val moviesFile = "src/main/resources/data/movies.json"

  private val spark = SparkSession
    .builder()
    .appName("read-movies")
    .config("spark.master", "local")
    .getOrCreate()

  private val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(moviesFile)

  moviesDF.printSchema()
  moviesDF.show()

  println(s"Total number of movies in the file are: ${moviesDF.count()}")
}
