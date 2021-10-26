package learn.spark.deploy

import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.functions._

/** Execute ./submit-job.sh from project home directory `PROJECT_HOME` which will
  *  - will build the jar using sbt assembly plugin
  *  - copies the jar to $PROJECT_HOME/spark-cluster/apps
  *  - starts spark containers using $PROJECT_HOME/spark-cluster/docker-compose.yml
  *  - finally submits the job with the following command
  *  {{{
  *    docker exec spark-master /spark/bin/spark-submit \
  *         --class learn.spark.deploy.TestDeploy \
  *         --master spark://spark-master:7077 \
  *         --deploy-mode client \
  *         --verbose --supervise \
  *         /opt/spark-apps/spark-tutorial.jar /opt/spark-data/movies.json /opt/spark-data/seeta-movies.json
  *  }}}
  */
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
    moviesDF.printSchema()
    moviesDF.show()
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
