import sbt._

object Dependencies {
  private val sparkVersion = "3.1.2"

  // https://github.com/apache/spark
  lazy private val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion

  // https://github.com/apache/spark
  lazy private val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  lazy private val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion

  lazy private val sparkSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

  // https://github.com/apache/spark/tree/master/external/kafka-0-10
  // Don't add kafka client dependencies, they are transitive
  // THIS IS FOR LOW LEVEL INTEGRATIONS
  lazy private val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

  lazy private val mongoScalaDriver = "org.mongodb.scala" %% "mongo-scala-driver" % "4.3.2"

  // https://github.com/scalatest/scalatest
  lazy private val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9" % "test"

  lazy private val compileDependencies: Seq[ModuleID] =
    Seq(mongoScalaDriver, sparkCore, sparkSql, sparkSqlKafka, sparkStreaming, sparkStreamingKafka)

  lazy private val testDependencies: Seq[ModuleID] = Seq(scalaTest)

  lazy val allDependencies: Seq[ModuleID] = compileDependencies ++ testDependencies
}