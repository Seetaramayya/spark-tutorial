package learn.spark.streaming

import learn.spark.domain.Objects.carsSchema
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

// Structured streaming
object ReadAndWriteToKafka {
  private val spark = SparkSession
    .builder()
    .appName("spark structured streaming with kafka integration play-ground")
    .master("local[2]")
    .getOrCreate()

  /** @param topicNames comma seperated topic names
    * @param bootstrapServers comma seperated bootstrap servers
    * @return
    */
  def readFromKafka(topicNames: String, bootstrapServers: String): Unit = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topicNames)
      .load()

    kafkaDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def carsDF: DataFrame = spark.readStream.schema(carsSchema).json("src/main/resources/data/cars")

  def writeToKafka(topicName: String, bootStrapServers: String): Unit = {
    val carsKafkaDF = carsDF.selectExpr("Name as key", "Name as value")
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServers)
      .option("topic", topicName)
      .option("checkpointLocation", "checkpoints")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def writeJsonToKafka(topicName: String, bootStrapServers: String): Unit = {
    val carsKafkaDF = carsDF.select(
      col("Name").as("key"),
      to_json(struct(col("Name"), col("Horsepower"), col("Origin"))).cast("String").as("value")
    )
    carsKafkaDF.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootStrapServers)
      .option("topic", topicName)
      .option("checkpointLocation", "checkpoints")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
//    readFromKafka("seeta-topic, test-topic", "localhost:9092, localhost:9093")
//    writeToKafka("test-topic", "localhost:9092")
    writeJsonToKafka("test-topic", "localhost:9092")
  }
}
