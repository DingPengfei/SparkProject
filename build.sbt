name := "SparkProject"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  // Spark dependency

  "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.1.1"
)