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
)
