package learn.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ DataFrame, DataFrameReader, Dataset, Encoders, Row, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

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

  implicit val stockOrdering: Ordering[Stock] = Ordering.fromLessThan { case (stockA, stockB) =>
    stockA.price < stockB.price
  }
}

object BasicRDDs {
  private val spark = SparkSession.builder().appName("RDD example").master("local[2]").getOrCreate()
  import spark.implicits._

  // spark context is required for RDD
  private val sc = spark.sparkContext

  private def loadDF(
      fileName: String,
      maybeSchema: Option[StructType] = None,
      basePath: String = "src/main/resources/data/"
  ): DataFrame = {
    val dataFrameReader: DataFrameReader = spark.read.option("header", "true")
    maybeSchema.fold(dataFrameReader.option("inferSchema", "true"))(dataFrameReader.schema).csv(s"$basePath/$fileName")
  }

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

    val schema: StructType             = Encoders.product[Stock].schema
    val stocksDF: DataFrame            = loadDF("stocks.csv", Some(schema))
    val stocksDSFromDF: Dataset[Stock] = stocksDF.as[Stock] // TODO Cannot up cast `date` from string to date.

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

    // RDD Transformation

    // 1. How many microsoft stocks exists?
    val totalMicrosoftStocks = stocksRDD
      .filter(_.symbol == "MSFT") // filter operation is lazy
      .count()                    // eager action

    println(s"Total Microsoft stocks are $totalMicrosoftStocks")

    // 2. Find all stock symbols
    val stockSymbols: RDD[String] = stocksRDD.map(_.symbol).distinct() // distinct is lazy operation
    stockSymbols.foreach(println)

    // 3. min and max stock values
    val minStock = stocksRDD.min() // requires implicit Ordering[T] which is available in companion
    println(s"Min stock is $minStock")
    println(s"Max stock is ${stocksRDD.max()}")

    // 4. grouping
    val averagePriceBySymbol: RDD[(String, Double)] = stocksRDD.groupBy(_.symbol).map { case (symbol, stocks) =>
      val count = stocks.size
      symbol -> stocks.map(_.price).sum / count
    }

    averagePriceBySymbol.foreach(println)
  }
}
