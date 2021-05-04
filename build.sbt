name := "spark-tutorial"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-mllib" % "2.4.5",

  "org.typelevel" %% "cats-effect" % "2.1.3",

  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
