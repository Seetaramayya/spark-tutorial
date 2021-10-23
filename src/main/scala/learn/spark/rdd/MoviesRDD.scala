package learn.spark.rdd

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ DoubleType, StringType, StructField, StructType }

import scala.util.Try

case class Movie(title: String, genre: String, rating: Double)

object Movie {
  implicit val ordering: Ordering[Movie] = Ordering.fromLessThan[Movie] { case (movieA, movieB) =>
    movieA.rating < movieB.rating
  }
  def createFrom(keyValues: Map[String, String]): Movie = {
    val title  = keyValues.get(RawMovie.keys(0)).filter(_ != "null").getOrElse("")
    val genre  = keyValues.get(RawMovie.keys(1)).filter(_ != "null").getOrElse("")
    val rating = Try(keyValues.get(RawMovie.keys(2)).filter(_ != "null").getOrElse("").toDouble).getOrElse(0.0d)
    Movie(title, genre, rating)
  }
}
case class RawMovie(title: Option[String], genre: Option[String], rating: Option[Double]) {
  def toMovie: Movie = Movie(title.getOrElse(""), genre.getOrElse(""), rating.getOrElse(0.0))
}
object RawMovie {
  val keys                                              = List("Title", "Major_Genre", "IMDB_Rating")
  implicit val movieFormatter: RootJsonFormat[RawMovie] = jsonFormat(RawMovie.apply, keys(0), keys(1), keys(2))
}
object MoviesRDD {
  private val spark = SparkSession.builder().appName("Movies RDD").master("local[2]").getOrCreate()
  private val sc    = spark.sparkContext

  private def readRDDFromFile(filePath: String): RDD[Movie] = sc
    .textFile(filePath)
    .map(_.parseJson.asJsObject.fields.mapValues(_.toString).filterKeys(RawMovie.keys.contains))
    .map(Movie.createFrom)

  private def readRDDFromDF(filePath: String): RDD[Movie] = {
    import spark.implicits._
    val schema = StructType(
      Array(
        StructField("Title", StringType),
        StructField("Major_Genre", StringType),
        StructField("IMDB_Rating", DoubleType)
      )
    )
    val df = spark.read.schema(schema).json(filePath)
    df.select(
      col("Title").as("title"),
      col("Major_Genre").as("genre"),
      col("IMDB_Rating").as("rating")
    ).where(
      col("title").isNotNull and col("genre").isNotNull and col("rating").isNotNull
    ).as[Movie]
      .rdd
  }

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    // Exercise 1. Read the movies.json as an RDD
    // val moviesRDD: RDD[Movie] = readRDDFromFile("src/main/resources/data/movies.json")
    val moviesRDD: RDD[Movie] = readRDDFromDF("src/main/resources/data/movies.json")

    // Exercise 0. Sort movies by descending order and print top 20
    moviesRDD.sortBy(_.rating, ascending = false).take(20)

    // Exercise 2. Show the distinct genres as an RDD
    moviesRDD.map(_.genre).distinct().filter(_ != "")

    // Exercise 3. Select all the Drama genre movies which are above 6 rating
    moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6).toDF().show()

    // Exercise 4. Show the average rating by genre
    moviesRDD
      .groupBy(_.genre)
      .map { case (genre, movies) =>
        val count = movies.size
        genre -> (movies.map(_.rating).sum / count)
      }
      .foreach(println)
  }
}
