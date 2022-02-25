package learn.spark.basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.util.Try

sealed trait MessageType
case class VDMType(message: String) extends MessageType
case class VDOType(message: String) extends MessageType

case class NMEAMessage(
    messageType: MessageType,
    numberOfFragments: Int,
    fragmentNumber: Int,
    sequenceNumber: Int,
    radioChannelCode: String,
    encodedPayload: String,
    checkFill: Int,
    checkSum: Int
)

object NMEAMessage {

  // [!AIVDM     , 1                , 1            ,                 ,B               ,33lol650000W5BhNawT@CrF40>`<,0*5C    ]
  // [MessageType, NumberOfFragments,FragmentNumber,getSequenceNumber,RadioChannelCode,EncodedPayload              ,Checksum]
  // Message Rules:
  //   1. MessageType field is mandatory, i.e., msg.split(",")(0) and it is always ends with VDM or VDO
  //   2. Message should start with ! and ends with *__ where dashes are base 16 chars such as [0-9A-Fa-f]
  //   3. Message should always have 7 fields (split by comma length == 7)
  //   4. Checksum is mandatory, i.e., msg.split(",")(6), Checksum split by '*', take second field convert to Integer with base 16

  private def unsafeGetField[T](errorMessage: String)(block: => T): T =
    Try(block).getOrElse(throw new IllegalArgumentException(errorMessage))

  /** If the given input is valid AIS message then it will create NMEAMessage otherwise throws exception
    * @param input given input
    * @return NMEAMessage or IllegalArgumentException
    */
  def unsafeFromString(input: String): NMEAMessage = {
    val raw: Array[String] = input.split(",")
    if (raw.length != 7) throw new IllegalArgumentException("not valid AIS message")
    val checksumArray = raw(6).split("\\*")
    if (checksumArray.length != 2) throw new IllegalArgumentException("invalid checksum field")
    val checksumFill = unsafeGetField("invalid checksum fill value")(checksumArray(0).toInt)
    val checksum     = unsafeGetField("invalid checksum")(Integer.parseInt(checksumArray(1), 16))

    val isValidMessageType: String => Boolean = message => message.startsWith("!") && message.length == 6

    val messageType = raw(0) match {
      case message if !isValidMessageType(message) => throw new IllegalArgumentException("not valid AIS message")
      case message if message.endsWith("VDM")      => VDMType(message.replaceAll("!", ""))
      case message if message.endsWith("VDO")      => VDOType(message.replaceAll("!", ""))
    }
    new NMEAMessage(messageType, raw(1).toInt, raw(2).toInt, raw(3).toInt, raw(4), raw(5), checksumFill, checksum)
  }

  def safeFromString(input: String): Either[IllegalArgumentException, NMEAMessage] =
    Try(unsafeFromString(input)).toEither.left.map(t => new IllegalArgumentException(t.getMessage))
}
object ReadAvroFile {
  private val spark = SparkSession.builder().appName("Read AVRO").master("local[2]").getOrCreate()
  private val avroFile =
    "/Users/ramayya/office/projects/timetoport/it/resources/mmsi_256767000_2020-08-03T08_03_49.277Z_2020-08-04T22_20_58.407Z.avro"

  def main(args: Array[String]): Unit = {
    println("Started")

    val avroSchema = StructType(
      Array(
        StructField("time", LongType),
        StructField("aisSource", StringType),
        StructField("nmeaMessages", ArrayType(StringType))
      )
    )
    val avroDF = spark.read.schema(avroSchema).format("avro").load(avroFile)
    avroDF.show(truncate = false)

    //
    // [!AIVDM     , 1                , 1            ,                 ,B               ,33lol650000W5BhNawT@CrF40>`<,0*5C    ]
    // [MessageType, NumberOfFragments,FragmentNumber,getSequenceNumber,RadioChannelCode,EncodedPayload              ,Checksum]
    // Message Rules:
    //   1. MessageType field is mandatory, i.e., msg.split(",")(0) and it is always ends with VDM or VDO
    //   2. Message should start with ! and ends with *__ where dashes are base 16 chars such as [0-9A-Fa-f]
    //   3. Message should always have 7 fields (split by comma length == 7)
    //   4. Checksum is mandatory, i.e., msg.split(",")(6), Checksum split by '*', take second field convert to Integer with base 16
    avroDF
      .withColumn("flags", col("nmeaMessage").getItem(0))
      .show(truncate = false)

    println(s"Total count is ${avroDF.count()}")
  }
}
