package learn.spark.basics

import learn.spark.domain.Objects.carsSchema
import org.apache.spark.sql.{ DataFrame, SparkSession }

/** This is an example of reading zipped source / compressed source
  * along with DateType parsing
  */
object ReadCompressedJson extends App {
  private val zipPath = "src/main/resources/data/cars.gz"
  private val spark = SparkSession
    .builder()
    .appName("Read compressed source example")
    .config("spark.master", "local[2]") // or .master("local[2]")
    .getOrCreate()

  // if the date format is not mentioned and it is not ISO date format the spark returns null
  val compressedDataFrame: DataFrame = spark.read
    .format("json")
    .schema(carsSchema)                 // if the field is in dateType then its format needs to be defined
    .option("dateFormat", "YYYY-MM-dd") // if the date format is not mentioned and spark returns null
    .option("compression", "gzip")      // possible values uncompressed, bzip2, gzip, lz4, snappy, deflate
    .load(zipPath)

  compressedDataFrame.show()
}
