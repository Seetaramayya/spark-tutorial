import sbt._

object Dependencies {

  // https://github.com/apache/spark
  lazy private val spark = "org.apache.spark" %% "spark-core" % "3.1.2"

  // https://github.com/scalatest/scalatest
  lazy private val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9" % "test"

  lazy private val compileDependencies: Seq[ModuleID] = Seq(spark)

  lazy private val testDependencies: Seq[ModuleID] = Seq(scalaTest)

  lazy val allDependencies: Seq[ModuleID] = compileDependencies ++ testDependencies
}
