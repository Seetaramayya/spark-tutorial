import Dependencies.allDependencies

ThisBuild / scalaVersion     := "2.12.14"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.spark.tutorial"
ThisBuild / organizationName := "spark-tutorial"

lazy val root = (project in file("."))
  .settings(
    name := "spark-tutorial",
    libraryDependencies ++= allDependencies
  )

