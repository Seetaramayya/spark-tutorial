package learn.spark.basics

import org.apache.spark.sql.SparkSession

/** Create a manual data frame describing smart phones
  *  - make
  *  - model
  *  - dimensions
  *  - camera pixels
  */
object SmartPhonesDataSet extends App {
  private val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("smart-phones")
    .getOrCreate()

  private val phones = Seq(
    ("Samsung Galaxy", "S21", "6.2 inches", "100px"),
    ("Apple", "iPhone", "6.1 inches", "10px")
  )
  private val manualSmartPhonesDF = spark.createDataFrame(phones)

  manualSmartPhonesDF.printSchema()
  manualSmartPhonesDF.show()

  import spark.implicits._
  private val manualSmartPhonesDFWithImplicits = phones.toDF("Make", "Model", "Dimensions", "Camera Pixels")
  manualSmartPhonesDFWithImplicits.printSchema()
}
