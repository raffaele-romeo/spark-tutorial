package quantexaflightsassignement

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object Transformation {
  def getLongestRun(countryX: String)(df: DataFrame): DataFrame = {
//    val fromX    = col("from") === countryX
//    val toX      = col("to") === countryX
//    val fromEqTo = col("from") === col("to")
//
//    val getCountriesForPansenger =
//      when(row_number().over(passengersWindow) === 1,
//           when(fromEqTo, when(fromX, 0).otherwise(1))
//             .when(fromX, when(toX, 0).otherwise(1))
//             .otherwise(when(!toX, 2).otherwise(1))).otherwise(
//        when(fromEqTo, 0).otherwise(when(!toX, 1).otherwise(0))
//      )

//    val passengersWindow = Window.partitionBy("passengerId").orderBy("date")
//    val windowForTakingLastRowInEachPartition =
//      Window.partitionBy("passengerId").orderBy(col("row_number").desc)
//
////    df.withColumn("lead", lead(col("from"), 1) over passengersWindow)
////      .where(col("lead").isNotNull and col("to") =!= col("lead"))
////      .show(false)
//
//    df.withColumn("row_number", row_number() over passengersWindow)
//      .withColumn("listOfCOuntries",
//                  collect_list(col("from")) over passengersWindow)
//      .withColumn("marker", when(rank.over(windowForTakingLastRowInEachPartition) === 1, "Y"))
//      .filter(col("marker") === "Y")  //After this filter we only have one value

    df.groupBy(col("passengerId"))
      .agg(collect_list("from").alias("listOfFrom"),
           collect_list("to").alias("listOfTo"))
      .withColumn("lastTo",
                  element_at(col("listOfTo"), size(col("listOfTo"))))
      .withColumn(
        "longestRun",
        calulateLongestRun(countryX)(col("listOfFrom"), col("lastTo")))
      .withColumn(
        "longestRunWithDuplicates",
        calulateLongestRunWithDuplicates(countryX)(col("listOfFrom"),
                                                   col("lastTo")))
      .select("passengerId", "longestRun", "longestRunWithDuplicates")
  }

  private def calulateLongestRun(countryX: String): UserDefinedFunction = {
    udf({ (listOfCountries: Seq[String], to: String) =>
      val completeListOfCountries = listOfCountries :+ to

      completeListOfCountries
        .scanLeft(List[String]())((v, s) =>
          if (s == countryX) List[String]() else v :+ s)
        .map(_.distinct)
        .map(_.size)
        .max
    })
  }

  private def calulateLongestRunWithDuplicates(
      countryX: String): UserDefinedFunction = {
    udf({ (listOfCountries: Seq[String], to: String) =>
      val completeListOfCountries = listOfCountries :+ to

      completeListOfCountries
        .scanLeft(List[String]())((v, s) =>
          if (s == countryX) List[String]() else v :+ s)
        .map(_.size)
        .max
    })
  }
}
