package learn.spark.sql

import learn.spark.utils.DataFrame
import org.apache.spark.sql.SparkSession

object BasicSparkSQL {
  private implicit val spark: SparkSession = SparkSession
    .builder()
    .config("spark.sql.legacy.createHiveTableByDefault", "false")
    .config("spark.sql.warehouse.dir", "target/warehouse")
    .appName("spark sql")
    .master("local[2]")
    .getOrCreate()
  private val carsDF = DataFrame.readJson("cars.json")

  // ANY SQL STATEMENTS CAN BE WRITTEN IN THE FOLLOWING WAY, THEY WILL BE EXECUTED FROM TO BOTTOM
  // Using spark SQL
  carsDF.createOrReplaceTempView("cars")
  private val americanCarsDF = spark.sql("""
      | select Name from cars where Origin = 'USA'
      |""".stripMargin)

  spark.sql("create database seeta")
  spark.sql("use seeta")
  spark.sql("create table persons(id integer, name string)")
  // All these SQL statements returns empty dataframe
  private val sqlDF     = spark.sql("""insert into persons values (1, "Martin Odersky"), (2, "Matei Zaharia")""")
  private val personsDF = spark.sql("select * from persons")
  def main(args: Array[String]): Unit = {
    // americanCarsDF.show()
    // sqlDF.show()
    personsDF.show()
  }
}
