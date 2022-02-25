package learn.spark.basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// TODO: https://opensky-network.org/api/flights/aircraft?icao24=406b48&begin=1635642061&end=1635840000
object AircraftDataAnalysis {
  private val filePath = "src/main/resources/data/aircraft-database.csv"
  private val spark    = SparkSession.builder().appName("AircraftData").master("local[2]").getOrCreate()
  private val aircraftDF = spark.read
    .option("header", "true")
    .csv(filePath)

  private val virginAtlanticDF = aircraftDF.filter(col("operatorcallsign").startsWith("VIR"))
  def main(args: Array[String]): Unit = {
    virginAtlanticDF.printSchema()
    virginAtlanticDF.filter(col("icao24") === "406b48").show()
  }
}
