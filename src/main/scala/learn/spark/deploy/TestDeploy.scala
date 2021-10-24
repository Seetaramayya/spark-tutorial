package learn.spark.deploy

import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.functions._

object TestDeploy {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage:  learn.spark.deploy.TestDeploy <json-file-path-to-read> <path-to-write-to>")
      System.exit(1)
    }
    // master will be mentioned as command line argument
    val spark = SparkSession.builder().appName("Deploying to Docker Spark Cluster").getOrCreate()
    println(s"Loading data from ${args(0)}")
    val moviesDF = spark.read.option("inferSchema", "true").json(args(0))
    println(s"Total records in moviesDF are ${moviesDF.count()}")
    val goodComediesDF = moviesDF
      .select(
        col("Title"),
        col("IMDB_Rating").as("Rating"),
        col("Release_Date").as("Release")
      )
      .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
      .orderBy(col("Rating").desc_nulls_last)

    goodComediesDF.show()

    moviesDF.write.mode(SaveMode.Overwrite).json(args(1))
  }
}
