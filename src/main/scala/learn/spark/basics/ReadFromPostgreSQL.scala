package learn.spark.basics

import org.apache.spark.sql.SparkSession

object ReadFromPostgreSQL extends App {
  private val spark = SparkSession
    .builder()
    .appName("Read from PostgreSQL")
    .master("local[2]")
    .getOrCreate()

  val user     = "admin"
  val password = "admin"
  val options = Map(
    "driver"   -> "org.postgresql.Driver",
    "url"      -> "jdbc:postgresql://localhost:5432/seeta",
    "user"     -> user,
    "password" -> password
  )

  private val jdbcDF = spark.read
    .format("jdbc")
    .options(options)
    .option("dbtable", "public.employees")
    .load()

  jdbcDF.show()
  jdbcDF.printSchema()
}
