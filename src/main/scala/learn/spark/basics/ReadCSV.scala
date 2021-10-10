package learn.spark.basics

import learn.spark.domain
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** In spark 3 and above date format is changed, previously it was using java
  * <a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html">simpleDateFormat</a> to
  * `java.time.format.DateTimeFormatter`
  *
  *  <ol>
  *    <li><a href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">DateTimeFormatter document</a></li>
  *    <li>
  *       data bricks migration <a href="https://docs.databricks.com/release-notes/runtime/7.x-migration.html">doc</a>
  *    </li>
  *  </ol>
  */
object ReadCSV extends App {
  private val path = "src/main/resources/data/stocks.csv"
  private val spark = SparkSession
    .builder()
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") // CORRECTED or LEGACY
    .appName("Reading CSV")
    .master("local[2]")
    .getOrCreate()

  private val stocksRawDataFrame = spark.read
    .schema(domain.Objects.stocksSchema)
    .option("header", "true")
    .option("nullValue", "")
    .option("dateFormat", "MMM d yyyy") // This format works for both LEGACY and CORRECTED
//    .option("dateFormat", "MMM dd YYYY") // LEGACY date format which will not work for new CORRECTED time parser
    .option("sep", ",")
    .csv(path)

  import spark.implicits._

  private val stocksDF = stocksRawDataFrame
    .select(
      $"symbol",
      col("date"),
      col("price")
    )

  stocksDF.show()
  stocksDF.printSchema()
}
