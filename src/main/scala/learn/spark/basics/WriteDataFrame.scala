package learn.spark.basics

import org.apache.spark.sql.{SaveMode, SparkSession}

object WriteDataFrame extends App {
  private val sourcePath = "src/main/resources/data/cars/cars.json"

  /** This wont be file, this will be a directory with all possible partitions and
    * some markers that says writing is success or failure
    */
  private val targetPath = "target/cars.json"

  val spark = SparkSession
    .builder()
    .appName("Writing DataFrame to File")
    .config("spark.master", "local[2]") //.master("local[2]") can be used as well
    .getOrCreate()
  // by default option("inferSchema", "true") is passed
  val carsDF = spark.read.json(sourcePath)

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save(targetPath)
}
