import cats.data.{Writer, WriterT}
import cats.effect.IO
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.LoggerFactory
import cats.implicits._

case class ID(age: Int)
object SparkLoggingWithMonad extends App{

  implicit val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("SparkLoggingWithMonad")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", 12)

  import spark.implicits._

  val data = spark.sparkContext.parallelize(1 to 2000000000).toDF("id")

  val logger = LoggerFactory.getLogger(getClass)

  type Logged[A] = WriterT[IO, Unit, A]

  def transformation1: Logged[Dataset[ID]] = {
    val transformation1 = data.map(x => ID(x.getAs[Int]("id")))
    WriterT(IO(logger.info("transformation1"), transformation1))
  }

  def filter(dataset: Dataset[ID])(f: ID => Boolean): Logged[Dataset[ID]] = {
    val transformation2 = dataset.filter(f)
    WriterT(IO(logger.info("transformation2"), transformation2))
  }

  def count(dataset: Dataset[ID]): Logged[Long] = {
    val transformation3 = dataset.count()
    WriterT(IO(logger.info(s"transformation3----------result: $transformation3"), transformation3))
  }

  val result = for {
    person          <- transformation1
    filtered        <- filter(person)(_.age > 2000)
    output          <- count(filtered)
  } yield output

  //val ((), output): (Unit, Long) = result.run.unsafeRunSync()


  type LoggedList[A] = Writer[List[String], A]

  def transformation1List: LoggedList[Dataset[ID]] = {
    val transformation1 = data.map(x => ID(x.getAs[Int]("id")))
    Writer(List("transformation1"), transformation1)
  }

  def filterList(dataset: Dataset[ID])(f: ID => Boolean): LoggedList[Dataset[ID]] = {
    val transformation2 = dataset.filter(f)
    Writer(List("transformation2"), transformation2)
  }

  def countList(dataset: Dataset[ID]): LoggedList[Long] = {
    val transformation3 = dataset.count()
    throw new Exception ("ciao")
    Writer(List(s"transformation3----------result: $transformation3"), transformation3)
  }

  val resultList = for {
    person          <- transformation1List
    filtered        <- filterList(person)(_.age > 2000)
    output          <- countList(filtered)
  } yield output

  val (list, outputList) = resultList.run

  println(list.mkString(";"))

}
