name := "altmetric-spark"

version := "1.0"

scalaVersion := "2.11.5"


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.10" % "1.2.0",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.2.0")

