name := "WebLogAnalysis"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  // spark
  "org.apache.spark"  %% "spark-core"      % sparkVersion,
  "org.apache.spark"  %% "spark-streaming" % sparkVersion,
  "org.apache.spark"  %% "spark-sql"       % sparkVersion,
  "org.apache.spark"  %% "spark-hive"      % sparkVersion,
  "org.apache.spark"  %% "spark-repl"      % sparkVersion,
  // config
  "com.typesafe" % "config" % "1.3.2"
  //  // log
//  "org.slf4j" % "log4j-over-slf4j" % "1.7.7",
//  "org.apache.logging.log4j" % "log4j-to-slf4j" % "2.8.2",
//  "org.slf4j" % "slf4j-api" % "1.7.7",
//  "ch.qos.logback" % "logback-classic" % "1.2.3",
//  // config
//  "com.typesafe" % "config" % "1.3.2"
)
