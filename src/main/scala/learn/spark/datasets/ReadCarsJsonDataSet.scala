package learn.spark.datasets

import learn.spark.utils.DataFrame
import org.apache.spark.sql.{ DataFrame, Dataset, Encoders, SparkSession }
import org.apache.spark.sql.functions._

import java.sql.Date

object ReadCarsJsonDataSet {
  private implicit val spark = SparkSession.builder().appName("data sets example").master("local[2]").getOrCreate()

  // dataset of a complex type, field names should be exact like input, that's is the reason Capital Name
  case class Car(
      Name: String,
      Miles_per_Gallon: Option[Double],
      Cylinders: Long,
      Displacement: Double,
      Horsepower: Option[Long],
      Weight_in_lbs: Long,
      Acceleration: Double,
      Year: Date,
      Origin: String
  )

  import spark.implicits._ // brings Encoders into the scope
  private val carsDF: DataFrame = DataFrame
    .readJson("cars.json")
    .select(
      $"Name",
      $"Miles_per_Gallon",
      $"Cylinders",
      $"Displacement",
      $"Horsepower",
      $"Weight_in_lbs",
      $"Acceleration",
      to_date($"Year", "yyyy-MM-dd").as("Year"),
      $"Origin"
    ) // read data frame

  private val carsDF2 = DataFrame
    .readJson("cars.json")
    .withColumn("Year", to_date($"Year", "yyyy-MM-dd"))

  private val carsDS = carsDF.as[Car] // converts to data set

  def main(args: Array[String]): Unit = {
    // carsDS.show()
    // carsDF2.as[Car].show()
    // Total number of cars
    val totalCars = carsDS.count()
    println(s"Total number of cars $totalCars")

    // Total number of cars which are powerful (HP > 140)
    println(s"Total number of powerful cars ${carsDS.filter(_.Horsepower.fold(false)(_ > 140)).count()}")

    val totalHorsePower: Long = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _)
    // Average HP for whole data set
    val average: Double = totalHorsePower / totalCars
    val average2        = carsDS.select(avg("Horsepower"))
    // TODO average is not same, check why
    println(s"Total HP: $totalHorsePower, Total cars: $totalCars, Average is $average")
    average2.show()

    // Group by key
    carsDS.groupByKey(_.Origin).count().show()
  }
}
