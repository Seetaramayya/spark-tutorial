package learn.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object CustomSchema extends App {
  private val fileName = "src/main/resources/data/cars.json"
  private val spark = SparkSession
    .builder()
    .appName("manual-schema")
    .config("spark.master", "local")
    .getOrCreate()

  private val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )
  private val dataFrame = spark.read
    .format("json")
    .schema(carsSchema)
    .load(fileName)

  dataFrame.printSchema()
  dataFrame.show()
}
