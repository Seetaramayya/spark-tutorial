package learn.spark.basics

import learn.spark.domain.Objects.carsSchema
import org.apache.spark.sql.SparkSession

object CustomSchema extends App {
  private val fileName = "src/main/resources/data/cars.json"
  private val spark = SparkSession
    .builder()
    .appName("manual-schema")
    .config("spark.master", "local")
    .getOrCreate()

  private val dataFrame = spark.read
    .format("json")
    .schema(carsSchema)
    .load(fileName)

  dataFrame.printSchema()
  dataFrame.show()
}
