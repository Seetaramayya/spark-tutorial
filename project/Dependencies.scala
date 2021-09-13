import sbt._

object Dependencies {
  private val sparkVersion = "3.1.2"

  // https://github.com/apache/spark
  lazy private val sparkCore = "org.apache.spark" %% "spark-core" % sparkVersion

  // https://github.com/apache/spark
  lazy private val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion

  // https://github.com/scalatest/scalatest
  lazy private val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9" % "test"

  lazy private val compileDependencies: Seq[ModuleID] = Seq(sparkCore, sparkSql)

  lazy private val testDependencies: Seq[ModuleID] = Seq(scalaTest)

  lazy val allDependencies: Seq[ModuleID] = compileDependencies ++ testDependencies
}
