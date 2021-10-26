import Dependencies.allDependencies
import sbtassembly.MergeStrategy

ThisBuild / scalaVersion := "2.12.14"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.spark.tutorial"
ThisBuild / organizationName := "spark-tutorial"
Global / onChangedBuildSource := ReloadOnSourceChanges

lazy val root = (project in file("."))
  .settings(
    name := "spark-tutorial",
    assembly / mainClass := Some("learn.spark.deploy.TestDeploy"),
    assembly / assemblyJarName := "spark-tutorial.jar",
    libraryDependencies ++= allDependencies
  )

ThisBuild / assemblyMergeStrategy := customMergeStrategy

lazy val customMergeStrategy: String => MergeStrategy = {
  case x if x.endsWith("UnusedStubClass.class") => MergeStrategy.discard
  case "module-info.class"                      => MergeStrategy.discard
  case "git.properties"                         => MergeStrategy.discard
  case x if x.contains("javax/inject/")         => MergeStrategy.last
  case x if x.contains("aopalliance")           => MergeStrategy.last
  case x if x.endsWith(".css")                  => MergeStrategy.first
  case "application.conf"                       => MergeStrategy.concat
  case PathList("META-INF", "aop.xml")          => MergeStrategy.deduplicate
  case x if x.contains("mime.types")            => MergeStrategy.first
  case x                                        => MergeStrategy.defaultMergeStrategy(x)
}
