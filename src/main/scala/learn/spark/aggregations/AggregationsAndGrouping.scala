package learn.spark.aggregations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsAndGrouping {
  private val moviesJsonPath = "src/main/resources/data/movies.json"
  private val spark = SparkSession
    .builder()
    .appName("Aggregations and grouping")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._
  private val moviesDF = spark.read.json(moviesJsonPath)

  // all the values except nulls
  private val genreCountDF    = moviesDF.select(count($"Major_Genre"))
  private val genreCountExpDF = moviesDF.selectExpr("count(Major_Genre)")

  // count distinct values of Major_Genre
  private val countDistinctGenreDF     = moviesDF.select(countDistinct(col("Major_Genre")))
  private val countDistinctGenreExprDF = moviesDF.selectExpr("count(distinct(Major_Genre))")

  // approximate distinct count
  private val approximateCountGenreDF = moviesDF.select(approx_count_distinct(col("Major_Genre")))
  private val approximateCountGenreExpDF =
    moviesDF.selectExpr("approx_count_distinct(Major_Genre) as `Approximate distinct count(Major_Genre)`")

  // counts all the rows including nulls
  private val allCountDF = moviesDF.select(count(col("*")))

  // Min, Max, Avg, StdDev
  private val movieStatisticsDF = moviesDF.selectExpr(
    "min(IMDB_Rating)",
    "max(IMDB_Rating)",
    "avg(IMDB_Rating)",
    "stddev(IMDB_Rating)",
    "sum(US_Gross) as `Total Gross`"
  )

  // Grouping select count(*) from moviesDF group by Major_Genre
  private val countByGenreDF     = moviesDF.groupBy($"Major_Genre").count()
  private val avgRatingByGenreDF = moviesDF.groupBy($"Major_Genre").avg("IMDB_Rating")
  private val aggByGenreDF = moviesDF
    .groupBy($"Major_Genre")
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy($"Avg_Rating".desc)

  // Sum up all the profits of all the movies
  private val calculateProfit = moviesDF.selectExpr("sum(US_Gross) as `Total_Profit`")

  // Count how many distinct directors are there in movies data
  private val distinctDirectors = moviesDF.selectExpr("count(distinct(Director)) as `Distinct_Directors`")

  // Calculate mean and sd for US gross revenue
  private val usStatisticsDF = moviesDF
    .selectExpr(
      "avg(US_Gross)",
      "stddev(US_Gross)"
    )

  // Calculate avg IMDB ratting and avg US_Gross per director
  private val statisticsByDirector = moviesDF
    .groupBy($"Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB"),
      avg("US_Gross").as("Avg_US_gross")
    )
    .orderBy($"Avg_IMDB".desc)

  def main(args: Array[String]): Unit = {
    calculateProfit.show()
    distinctDirectors.show()
    usStatisticsDF.show()
    statisticsByDirector.show()
  }
}
