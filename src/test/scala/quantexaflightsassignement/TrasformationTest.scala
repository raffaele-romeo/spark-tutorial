package quantexaflightsassignement

import java.sql.Date

import org.scalatest.{FunSpec, Matchers}
import Transformation.getLongestRun

class TrasformationTest
    extends FunSpec
    with Matchers
    with SparkImplicits {

  describe("countCountriesSince") {

    it("counts countries since target country") {
      import spark.implicits._
      val inputDF =
        List(
          // passenger 1 flights a->b->X->X->c (already ordered in df)
          (1, Date.valueOf("2017-01-01"), "a", "b"),
          (1, Date.valueOf("2017-01-02"), "b", "X"),
          (1, Date.valueOf("2017-01-03"), "X", "X"),
          (1, Date.valueOf("2017-01-04"), "X", "c"),
          // passenger 2 flights X->a->X (already ordered in df)
          (2, Date.valueOf("2017-01-01"), "X", "a"),
          (2, Date.valueOf("2017-01-02"), "a", "X"),
          // passenger 3 flights X->a->b-X (out of order)
          (3, Date.valueOf("2017-01-02"), "a", "b"),
          (3, Date.valueOf("2017-01-01"), "X", "a"),
          (3, Date.valueOf("2017-01-03"), "b", "X"),
          // passenger 4 flights X->a->a->X
          (4, Date.valueOf("2017-01-01"), "X", "a"),
          (4, Date.valueOf("2017-01-02"), "a", "a"),
          (4, Date.valueOf("2017-01-03"), "a", "X"),
          // passenger 5 flights a -> X -> b -> c
          (5, Date.valueOf("2017-01-01"), "a", "X"),
          (5, Date.valueOf("2017-01-02"), "X", "b"),
          (5, Date.valueOf("2017-01-03"), "b", "c"),
          // passenger 6 flights a -> a
          (6, Date.valueOf("2017-01-01"), "a", "a"),
          (7, Date.valueOf("2017-01-04"), "cn", "X"),
          (7, Date.valueOf("2017-01-05"), "X", "de"),
          (7, Date.valueOf("2017-01-06"), "de", "X"),
          //Good test case for testing duplicates
          (8, Date.valueOf("2017-01-04"), "dk", "tj"),
          (8, Date.valueOf("2017-01-05"), "tj", "jo"),
          (8, Date.valueOf("2017-01-06"), "jo", "X"),
          (8, Date.valueOf("2017-01-07"), "X", "ar"),
          (8, Date.valueOf("2017-01-08"), "ar", "ch"),
          (8, Date.valueOf("2017-01-09"), "ch", "ca"),
          (8, Date.valueOf("2017-01-10"), "ca", "ir"),
          (8, Date.valueOf("2017-01-11"), "ir", "ca"),
          (8, Date.valueOf("2017-01-12"), "ca", "ch"),
          (8, Date.valueOf("2017-01-13"), "ch", "ar"),
          (8, Date.valueOf("2017-01-14"), "ar", "X")
        ).toDF("passengerId", "date", "from", "to")

      val expectedDF =
        List(
          (1, 2),
          (2, 1),
          (3, 2),
          (4, 1),
          (5, 2),
          (6, 1),
          (7, 1),
          (8, 4),
        ).toDF("passengerId", "longestRun")

      val actualDF = getLongestRun("X")(inputDF).orderBy("passengerId")

      actualDF.collect() should be( expectedDF.collect())

    }
  }
}
