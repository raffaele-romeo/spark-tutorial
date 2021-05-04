package quantexaflightsassignement

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

object IO {
  def csvReader(path: String)(
      implicit sparkSession: SparkSession): DataFrame = {
    sparkSession.read
      .option("header", value = true)
      .csv(s"$path")
  }

  def getAbsolutePath(relativePath: String): String = {
    val resource = getClass.getResource(relativePath)

    new File(resource.toURI).getAbsolutePath
  }
}

trait SparkImplicits {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("FlightAssignments")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  spark.conf.set("spark.sql.shuffle.partitions", 12)
}
