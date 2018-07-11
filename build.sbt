name := "main/scala/error"

version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-sql"  % "2.1.0",
  "org.apache.spark" %% "spark-mllib" % "0.9.0-incubating"
)


scalaVersion := "2.10.5"
