package learn.spark.basics

import org.apache.spark.sql.{ Column, SparkSession }
import org.apache.spark.sql.functions._

object ComplexTypes {
  private val carsPath = "src/main/resources/data/cars/cars.json"
  private val spark = SparkSession
    .builder()
    .appName("Complex types")
    .master("local[2]")
    .getOrCreate()

  private val carNames: List[String] = List("volkswagen", "ford")
  private val carsDF                 = spark.read.json(carsPath)
  private val regex                  = carNames.mkString("|")
  private val matchingCarsDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regex, 0).as("matched_car")
    )
    .filter(col("matched_car") =!= "")
    .drop(col("matched_car"))

  private val mapNamesToColumn: List[Column] = carNames.map(name => col("Name").contains(name))
  private val volkswagenOrFordFilter         = mapNamesToColumn.fold(lit(false))(_ or _)
  private val matchingCarsDF2                = carsDF.filter(volkswagenOrFordFilter)

  def main(args: Array[String]): Unit = {
    matchingCarsDF2.show()
  }

}
