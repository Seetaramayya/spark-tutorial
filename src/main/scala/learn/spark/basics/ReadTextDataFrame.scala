package learn.spark.basics

import org.apache.spark.sql.SparkSession

object ReadTextDataFrame extends App {
  private val path = "src/main/resources/data/sampleTextFile.txt"
  private val spark = SparkSession
    .builder()
    .appName("Reading Text file")
    .master("local[2]")
    .getOrCreate()

  val df = spark.read.text(path)
  df.show()
  df.printSchema()
}
