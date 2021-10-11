package learn.spark.basics

import org.apache.spark.sql.{ SaveMode, SparkSession }

object PostgreSQL extends App {
  private val moviesJsonPath  = "src/main/resources/data/movies.json"
  private val moviesTableName = "public.movies"
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

  private val employeesDF = spark.read
    .format("jdbc")
    .options(options)
    .option("dbtable", "public.employees")
    .load()

  private val moviesDF = spark.read.json(moviesJsonPath)
  println(s"Total ${moviesDF.count()} are inserted into '$moviesTableName'")
  // write as postgress table "public.movies"

  // TODO: SURPRISING FACT: Save mode was Overwrite -> Append -> Overwrite
  // I was expecting 6402 records because, 3201     -> 6402   -> 6402 (it override existing values so nothing to write)
  // That means existing 6402 should be there but I see 3201.
  moviesDF.write
    .format("jdbc")
    .options(options)
    .mode(SaveMode.Overwrite)
    .option("dbtable", moviesTableName)
    .save()
}
