package learn.spark.datasets

import org.apache.spark.sql.{ Dataset, Encoder, Encoders, SparkSession }

object ReadNumbersCSV {
  private val spark = SparkSession.builder().appName("data sets example").master("local[2]").getOrCreate()
  private val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true") // this is important otherwise cast issues raises
    .csv("src/main/resources/data/numbers.csv")

  // convert a DF to a Dataset
  // NO NEED TO DEFINE IMPLICIT ENCODERS LIKE THIS, import spark.implicit._
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int]           = numbersDF.as[Int]

  def main(args: Array[String]): Unit = {
    numbersDS.show()
  }
}
