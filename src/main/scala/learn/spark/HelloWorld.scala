package learn.spark

import org.apache.spark.sql.SparkSession

object HelloWorld extends App {
  val fileName = "src/main/resources/data/cars.json"
  val spark = SparkSession
    .builder()
    .appName("hello-world")
    .config("spark.master", "local")
    .getOrCreate()

  // automatically infer the schema but inferring might be wrong, see Year inferred as String
  val dataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load(fileName)

  dataFrame.show()
  dataFrame.printSchema()

  dataFrame.take(10).foreach(println)

}
