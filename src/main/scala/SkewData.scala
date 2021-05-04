import org.apache.spark.sql.functions._

import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

import scala.util.Random
import scala.math.BigDecimal

case class MakeModel(make: String, model: String)

case class T1(registration: String,
              make: String,
              model: String,
              engineSize: BigDecimal)

case class T2(make: String,
              model: String,
              engineSize: BigDecimal,
              salePrice: Double)

object SkewData extends App {

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

//  val skewedJoin = t1
//    .join(t2, Seq("make", "model"))
//    .filter(abs(t2("engineSize") - t1("engineSize")) <= BigDecimal("0.1"))
//    .groupBy("registration")
//    .agg(avg("salePrice").as("averagePrice"))
//    .collect()

  //Method 1
//  val nonSkewedResultMethod1 = t1
//    .withColumn("engineSize",
//                explode(
//                  array($"engineSize" - BigDecimal("0.1"),
//                        $"engineSize",
//                        $"engineSize" + BigDecimal("0.1"))))
//    .join(t2, Seq("make", "model", "engineSize"))
//    .groupBy("registration")
//    .agg(avg("salePrice").as("averagePrice"))
//    .collect()

  //Method 2
  val t1WithSkewKey = t1.withColumn(
    "skewKey",
    explode(
      when($"make" === "FORD" && $"model" === "FIESTA",
           lit((0 to 199).toArray)).otherwise(array(lit(0)))))

  val t2WithSkewKey = t2.withColumn(
    "skewKey",
    when($"make" === "FORD" && $"model" === "FIESTA",
         monotonically_increasing_id() % 200).otherwise(lit(0)))

  val nonSkewedResultMethod2 = t1WithSkewKey
    .join(t2WithSkewKey, Seq("make", "model", "skewKey"))
    .filter(
      abs(t2("engineSize") - t1("engineSize")) <= BigDecimal("0.1"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))
    .collect()
}

case class MakeModelGenerator(implicit sparkSession: SparkSession) {
  import sparkSession.implicits._

  val makeModelSet = List(
    MakeModel("FORD", "FIESTA"),
    MakeModel("NISSAN", "QASHQAI"),
    MakeModel("HYUNDAI", "I20"),
    MakeModel("SUZUKI", "SWIFT"),
    MakeModel("MERCEDED_BENZ", "E CLASS"),
    MakeModel("VAUXHALL", "CORSA"),
    MakeModel("FIAT", "500"),
    MakeModel("SKODA", "OCTAVIA"),
    MakeModel("KIA", "RIO")
  )

  def randomMakeModel(): MakeModel = {
    val makeModelIndex =
      if (Random.nextBoolean()) 0 else Random.nextInt(makeModelSet.size)
    makeModelSet(makeModelIndex)
  }

  def randomEngineSize(): BigDecimal =
    BigDecimal(s"1.${Random.nextInt(9)}")

  def randomRegistration(): String =
    s"${Random.alphanumeric.take(7).mkString("")}"

  def randomPrice(): Int = 500 + Random.nextInt(5000)

  def randomT1(): T1 = {
    val makeModel = randomMakeModel()
    T1(randomRegistration(),
       makeModel.make,
       makeModel.model,
       randomEngineSize())
  }

  def randomT2(): T2 = {
    val makeModel = randomMakeModel()
    T2(makeModel.make, makeModel.model, randomEngineSize(), randomPrice())
  }

}
