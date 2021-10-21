package learn.spark.basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManageNulls {
  private val spark    = SparkSession.builder().appName("Manage nulls").master("local[2]").getOrCreate()
  private val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  import spark.implicits._

  // select first non null value
  private val firstNonNullValue = moviesDF
    .select(
      $"Title",
      $"Rotten_Tomatoes_Rating",
      $"IMDB_Rating",
      coalesce($"Rotten_Tomatoes_Rating", $"IMDB_Rating")
    )

  // select only non null values
  private val nonNullRottenTomatoesDF = moviesDF.select("*").where($"Rotten_Tomatoes_Rating".isNotNull)

  // removing nulls
  private val removedNullsDF = moviesDF.select($"Title", $"IMDB_Rating").na.drop()

  // replace nulls
  private val replacedNullsDF = moviesDF.na.fill(Map("IMDB_Rating" -> 0, "Director" -> "Unknown"))

  // Only possible in SQL
  private val fewExprFunctionsDF = moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating) as nvl",       // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating) as nullif", // Returns if two values are equal otherwise first value
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating, 0) as nvl2"   // if (first != null) second else third
  )

  def main(args: Array[String]): Unit = {
    // moviesDF.show()
    // firstNonNullValue.show()
    // nonNullRottenTomatoesDF.show()
    // removedNullsDF.show()
    // replacedNullsDF.show()
    fewExprFunctionsDF.show()
  }
}
