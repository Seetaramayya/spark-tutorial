package learn.spark.streaming

import learn.spark.domain.Objects.Person
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import java.sql.Date

object PlayGround {

  private val HOST = "localhost"
  val kafkaParams = Map[String, Object](
    "bootstrap.servers"  -> "localhost:9092",
    "key.deserializer"   -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id"           -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset"  -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val BATCH_INTERVAL_IN_SECONDS = 2
  val spark                     = SparkSession.builder().appName("play-ground").master("local[2]").getOrCreate()
  val ssc                       = new StreamingContext(spark.sparkContext, Seconds(BATCH_INTERVAL_IN_SECONDS))
  val path                      = "src/main/resources/data"
  ssc.fileStream(path)

  private val LISTENING_SOCKET = 9090

  def readPeople(): DStream[Person] = ssc.socketTextStream(HOST, LISTENING_SOCKET).map { line =>
    val fields = line.split(":")
    Person(
      fields(0).toInt,         // id
      fields(1),               // first name
      fields(2),               // middle name
      fields(3),               // last name
      fields(4),               // gender
      Date.valueOf(fields(5)), // birth
      fields(6),               // ssn/uuid
      fields(7).toInt          // salary
    )
  }

  def peopleWith(filterCondition: Person => Boolean): DStream[Person] = readPeople().filter(filterCondition)

  def readLinesFromSocket(): DStream[String] = ssc.socketTextStream(HOST, LISTENING_SOCKET)

  def computeWordOccurrencesByWindow(): DStream[(String, Int)] = {
    ssc.checkpoint("checkpoints")
    readLinesFromSocket()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b, // reduce function
        (a, b) => a - b, // inverse function
        Seconds(30),     // window interval
        Seconds(10)      // sliding interval
      )
  }

  /** word longer than 10 characters >= 2$ otherwise 0$
    * Input text into the terminal, calculate money mad in 30 seconds, update every 10 seconds
    * @return
    */
  def earnedMoney(): DStream[Long] = {
    ssc.checkpoint("money-checkpoint")
    readLinesFromSocket()
      .flatMap(_.split(" "))
      .filter(_.length >= 10)
      .countByWindow(
        Seconds(30), // window interval
        Seconds(10)  // sliding interval
      )
      .map(_ * 2)
  }

  // 1632645998000
  // 1632646008000
  // 1632646018000
  // 1632646028000

  def main(args: Array[String]): Unit = {
    earnedMoney().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
