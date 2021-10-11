package learn.spark.joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object GuitarJoins {
  private val spark = SparkSession
    .builder()
    .appName("Joins")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._

  private val guitarsDF    = spark.read.json("src/main/resources/data/guitars.json")
  private val guitaristsDF = spark.read.json("src/main/resources/data/guitarPlayers.json")
  private val bandsDF      = spark.read.json("src/main/resources/data/bands.json")
  private val bandId       = bandsDF.col("id")
  private val guitarBandId = guitaristsDF.col("band")

  // inner joins
  private val guitaristsBandDF = guitaristsDF.join(bandsDF, guitarBandId === bandId, "inner")

  // left outer join: everything inner join + all rows in the LEFT table with nulls if the data is missing in the right
  private val guitaristsLeftJoinBandDF = guitaristsDF.join(bandsDF, guitarBandId === bandId, "left_outer")

  // right outer join: everything inner join + all rows in the RIGHT table with nulls if the data is missing in the left
  private val guitaristsRightJoinBandDF = guitaristsDF.join(bandsDF, guitarBandId === bandId, "right_outer")

  // outer join: everything inner join + all rows in the RIGHT table with nulls if the data is missing in the left
  private val guitaristsOuterJoinBandDF = guitaristsDF.join(bandsDF, guitarBandId === bandId, "outer")

  def main(args: Array[String]): Unit = {
    guitaristsBandDF.show()
    guitaristsLeftJoinBandDF.show()
    guitaristsRightJoinBandDF.show()
    guitaristsOuterJoinBandDF.show()

    // everything in the left DF for which there is a row in the right DF satisfying the condition
    guitaristsDF.join(bandsDF, guitarBandId === bandId, "left_semi").show()

    // everything in the left DF for which there is NO row in the right DF satisfying the condition
    guitaristsDF.join(bandsDF, guitarBandId === bandId, "left_anti").show()

  }
}
