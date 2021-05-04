import SkewData.spark
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random
import scala.math.BigDecimal

object SparkUnpersist extends App {
  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("SkewData")
    .getOrCreate()

  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  spark.conf.set("spark.sql.shuffle.partitions", 12)

  import spark.implicits._

  val numberOfPartitions = 12 //spark.sparkContext.defaultParallelism
  val numberOfT1         = 10000
  val numberOfT2         = 100000

  val makeModelGenerator = MakeModelGenerator()
  val t1 = RandomDataframeGenerator.generateDataset(numberOfT1,
    numberOfPartitions,
    makeModelGenerator.randomT1())
  val t2 = RandomDataframeGenerator.generateDataset(numberOfT2,
    numberOfPartitions,
    makeModelGenerator.randomT2())

    val skewedJoin = t1
      .join(t2, Seq("make", "model"))
      .filter(abs(t2("engineSize") - t1("engineSize")) <= BigDecimal("0.1"))
      .groupBy("registration")
      .agg(avg("salePrice").as("averagePrice"))
      .cache()

  val datasets: List[DataFrame] = List(0,1,2,3).map {
    int => skewedJoin.withColumn(s"random$int", lit(int))
  }

  //skewedJoin.unpersist(true)

  skewedJoin.collect()
  datasets.map(_.collect())

}
