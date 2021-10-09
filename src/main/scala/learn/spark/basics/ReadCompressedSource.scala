package learn.spark.basics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** This is an example of reading zipped source / compressed source
  * along with DateType parsing
  */
object ReadCompressedSource extends App {
  private val zipPath = "src/main/resources/data/cars.gz"
  private val spark = SparkSession
    .builder()
    .appName("Read compressed source example")
    .config("spark.master", "local[2]") // or .master("local[2]")
    .getOrCreate()

  val compressedDataFrame: DataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .option("dateFormat", "YYYY-MM-dd") //
    .option("compression", "gzip")      // possible values
    .load(zipPath)

  compressedDataFrame.show()
}
