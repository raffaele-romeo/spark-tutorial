package quantexaflightsassignement

import IO._
import Transformation._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._

// In the data, passenger can visit same country multiple time so we want a distinct count for the countries
object FlightsAssignment extends App with SparkImplicits {
  val flightDataPath = "/data/flightData.csv"
  val passengersPath = "/data/passengers.csv"

  val flightData = csvReader(getAbsolutePath(flightDataPath))
  val passengersData = csvReader(getAbsolutePath(flightDataPath))

  object Q3 {
    def run: Dataset[Row] = flightData
      .transform(getLongestRun("uk")).orderBy(desc("longestRunWithDuplicates"))
  }

  Q3.run.where(col("longestRun") =!= col("longestRunWithDuplicates")).show(false)
  println("COUNT +++++++" + Q3.run.count())  //15500
}
