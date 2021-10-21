package learn.spark.utils

import org.apache.spark.sql.{ DataFrame, SparkSession }

object DataFrame {
  def readJson(fileName: String, basePath: String = "src/main/resources/data")(implicit
      spark: SparkSession
  ): DataFrame =
    spark.read.option("inferSchema", "true").json(s"$basePath/$fileName")
}
