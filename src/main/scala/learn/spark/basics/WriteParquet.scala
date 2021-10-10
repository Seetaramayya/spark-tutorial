package learn.spark.basics

import org.apache.spark.sql.{ SaveMode, SparkSession }

object WriteParquet extends App {
  private val source         = "src/main/resources/data/cars/cars.json"
  private val targetLocation = "target/cars.parquet"
  private val spark = SparkSession
    .builder()
    .appName("Write in parquet format")
    .master("local[2]")
    .getOrCreate()

  private val carsDF = spark.read.json(source)
  carsDF.write.mode(SaveMode.Overwrite).parquet(targetLocation)
}
