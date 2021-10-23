package learn.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, Dataset, Row, SparkSession }
import org.apache.spark.sql.functions._

import java.time.format.DateTimeFormatter
import java.time.{ LocalDate, LocalDateTime }
import scala.util.{ Failure, Success, Try }

case class Stock(symbol: String, date: LocalDate, price: Double)

object Stock {
  private val formatter = DateTimeFormatter.ofPattern("MMM d yyyy")
  def fromLine(line: String, separatorRegex: String = ","): Option[Stock] = Try {
    val tokens = line.split(separatorRegex)
    Stock(tokens(0), LocalDate.parse(tokens(1), formatter), tokens(2).toDouble)
  } match {
    case Success(stock) => Some(stock)
    case Failure(exception) =>
      println(s"For $line => ${exception.getMessage}")
      None
  }
}

object BasicRDDs {
  private val spark = SparkSession.builder().appName("RDD example").master("local[2]").getOrCreate()
  import spark.implicits._

  // spark context is required for RDD
  private val sc = spark.sparkContext

  private def loadDF(fileName: String, basePath: String = "src/main/resources/data/"): DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(fileName)

  private def convertToStock(line: String): Stock = ???
  def main(args: Array[String]): Unit = {
    // Convert Regular scala collection into RDD
    val regularCollection: Seq[Long] = (1L to 10000000L)
    val numbersRDD: RDD[Long]        = sc.parallelize(regularCollection)
    numbersRDD.toDF().show() // requires spark.implicits._ import
    println(s"Total number of elements are => ${numbersRDD.count()}")

    // Read RDD from a file
    val stocksRDD: RDD[Stock] = sc
      .textFile("src/main/resources/data/stocks.csv")
      .map(line => Stock.fromLine(line))
      .collect { case Some(stock) => stock }

    val stocksDF: DataFrame            = loadDF("stocks.csv")
    val stocksDSFromDF: Dataset[Stock] = stocksDF.as[Stock]

    // Dataset -> RDD
    val stocksRDDFromDS: RDD[Stock] = stocksDF.as[Stock].rdd

    // Dataframe -> RDD
    val stocksRDDFromDF: RDD[Row] = stocksDF.rdd

    // Convert RDD[Stock] -> DataFrame looses type safety because DataFrame is an alias to Dataset[Row]
    val stocksDF2: DataFrame = stocksRDD.toDF()
    val averagePriceBySymbolDF: DataFrame = stocksDF2
      .groupBy(col("symbol"))
      .agg(
        avg(col("price")).as("Average Price")
      )
      .orderBy(col("Average Price").desc)

    averagePriceBySymbolDF.show()

    // Convert RDD[Stock] -> Dataset[Stock]
    val stocksDS: Dataset[Stock] = spark.createDataset(stocksRDD)

    stocksDS.show()
    println(s"Total number of stocks are ${stocksRDD.count()}")

  }
}
