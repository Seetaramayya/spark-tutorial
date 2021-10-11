package learn.spark.basics

import org.apache.spark.sql.{ SaveMode, SparkSession }

object WriteTabSeperatedCSV extends App {
  private val moviesJsonPath = "src/main/resources/data/movies.json"
  private val targetCSVPath  = "target/movies.csv"
  private val spark = SparkSession
    .builder()
    .appName("Write CSV with tab separator")
    .master("local[2]")
    .getOrCreate()

  private val dataframe = spark.read.json(moviesJsonPath)
  dataframe.printSchema()
  dataframe.show()

  // write as CSV
  dataframe.write
    .mode(SaveMode.Append)
    .option("sep", "\t")
    .option("nullValue", "")
    .csv(targetCSVPath)
}
