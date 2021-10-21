package learn.spark.datasets

import learn.spark.utils.DataFrame
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.sql.functions._

object Joins {
  private implicit val spark = SparkSession.builder().appName("data sets example").master("local[2]").getOrCreate()
  import spark.implicits._ // encoders are brought into scope which are used to convert DF to DS

  // if you use any keywords then those needs to be surrounded by backticks, for example: `type`
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  private val guitarsDS       = DataFrame.readJson("guitars.json").as[Guitar]
  private val guitarPlayersDS = DataFrame.readJson("guitarPlayers.json").as[GuitarPlayer]
  private val bandsDS         = DataFrame.readJson("bands.json").as[Band]

  private val guitarPlayerBandDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS
    .joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"))

  private val someDS =
    guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")

  def main(args: Array[String]): Unit = {
    guitarPlayerBandDS.show()
    someDS.show()
  }
}
