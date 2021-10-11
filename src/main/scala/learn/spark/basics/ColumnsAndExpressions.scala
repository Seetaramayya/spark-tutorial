package learn.spark.basics

import org.apache.spark.sql.{ Column, DataFrame, SparkSession }
import org.apache.spark.sql.functions._

object ColumnsAndExpressions {
  private val carsPath     = "src/main/resources/data/cars/cars.json"
  private val moreCarsPath = "src/main/resources/data/more_cars.json"
  private val spark = SparkSession
    .builder()
    .appName("Columns and expressions")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._

  private val carsDF: DataFrame = spark.read.json(carsPath)
  // 1. this is one way to select column Name
  private val NameColumn = carsDF.col("Name")

  private val carsNameDF = carsDF.select(NameColumn)
  // 2. this is another way to select column Cylinders
  private val selectedDF = carsDF.select(
    col("Cylinders"),        // It requires functions import
    column("Weight_in_lbs"), // It requires functions import
    $"Year",                 // $ interpolation it requires import spark.implicits._
    'Horsepower,             // scala symbol auto converted to column
    expr("Origin")
  )

  // selecting multiple column names as strings works but mixing with col or any functions does not work
  private val carsSelectWithNameDF = carsDF.select("Name", "Origin")

  private val carsWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs").as("Weight in Pounds"),
    (col("Weight_in_lbs") / 2.2).as("Weight in KG"),
    expr("Weight_in_lbs / 2.2").as("Weight in KG expr magic") // string will be converted to expression
  )

  private val carsWeightExpDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // adding new column
  private val carsWithNewColumnDF = carsDF.withColumn("Weight_in_kg_3", expr("Weight_in_lbs / 2.2"))

  // rename existing column
  private val carsRenameColumnDF = carsDF
    .withColumnRenamed("Weight_in_lbs", "Weight in pounds")
    .withColumn("Weight in KGs", expr("`Weight in pounds` / 2.2"))

  // drop can be used to delete a column from DF

  // Use backticks when selecting columns with spaces
  private val carsWeightWithBacktickDF = carsRenameColumnDF.selectExpr(
    "Name",
    "`Weight in pounds`",
    "`Weight in KGs`"
  )

  private def filteringExamples(): Unit = {
    val europeanCarsDF  = carsDF.filter(col("Origin") === "Europe") // pay attention on triple equals
    val europeanCarsDF2 = carsDF.filter("Origin = 'Europe'")        // in the expression use single quotes

    val americanPowerfulCarsDF  = carsDF.filter("Origin = 'USA' and Horsepower > 150")
    val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
    val americanPowerfulCarsDF3 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
    println(s"Total powerful american cars are: ${americanPowerfulCarsDF2.count()}")
    americanPowerfulCarsDF2.show()
  }

  private def unionExamples(): Unit = {
    val moreCarsDF: DataFrame = spark.read.json(moreCarsPath)
    val allCarsDF             = carsDF.union(moreCarsDF)
    allCarsDF.select("Origin").distinct().show()
  }

  private val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  private val readMoviesAndSelectTitleAndRelease: DataFrame = moviesDF
    .select(
      'Title,
      $"Release_Date".as("Release Date")
    )

  private val withTotalProfit: DataFrame = moviesDF
    .withColumn(
      "Total Profit",
      when($"US_Gross".isNull, 0) + when($"Worldwide_Gross".isNull, 0) + when($"US_DVD_Sales".isNull, 0)
    )
    .select(
      $"Title",
      $"US_Gross".as("USA Gross"),
      $"Worldwide_Gross".as("Worldwide Gross"),
      $"US_DVD_Sales".as("USA DVD Sales"),
      $"`Total Profit`"
    )

  // TODO : I do not know how to use when in expr
  private val withTotalProfit2: DataFrame = moviesDF
    .selectExpr(
      "Title",
      "US_Gross as `USA Gross`",
      "Worldwide_Gross as `Worldwide Gross`",
      "US_DVD_Sales as `USA DVD Sales`",
      "US_Gross + Worldwide_Gross as `Total Profit`"
    )

  private def goodComedyMovies: DataFrame =
    moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 7").orderBy($"IMDB_Rating".desc, $"Title")

  def main(args: Array[String]): Unit = {
    // carsNameDF.show()
    // selectedDF.show()
    // carsWeightDF.show()
    // carsWeightExpDF.show()
    // carsWithNewColumnDF.show() // shows all the columns along with existing columns
    // carsRenameColumnDF.show()
    // carsWeightWithBacktickDF.show()
    // filteringExamples()
    // unionExamples()
    // readMoviesAndSelectTitleAndRelease.show()
    // withTotalProfit.show()
    withTotalProfit2.show()
    // goodComedyMovies.show()
  }
}
