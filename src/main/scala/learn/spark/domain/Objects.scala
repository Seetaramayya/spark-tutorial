package learn.spark.domain

import org.apache.spark.sql.types._

import java.sql.Date

object Objects {
  case class Person(
      id: Int,
      firstName: String,
      middleName: String,
      lastName: String,
      gender: String,
      birthDate: Date,
      ssn: String,
      salary: Int
  )

  val carsSchema: StructType = StructType(
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
}
