scalaVersion := "2.13.12"

name := "minhash"
organization := "ca.uvic.mmds"
version := "1.0"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")

//libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)
