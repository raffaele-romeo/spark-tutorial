import java.sql.Timestamp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

object RandomDataframeGenerator {

  def generateDataframeWithId(numberOfRecords: Int,
                              numberOfPartitions: Int)(
      implicit sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._

    val partitionSize =
      Math.ceil(numberOfRecords.toDouble / numberOfPartitions).toInt

    (0 until numberOfPartitions)
      .toDF("partitionId")
      .repartition(numberOfPartitions)
      .flatMap(row => {
        val partitionId         = row.getInt(0)

        ((partitionId * partitionSize) until (partitionId + 1) * partitionSize).toIterator
      }).toDF()
  }

  def generateDataset[T](numberOfRecords: Int,
                   numberOfPartitions: Int,
                   randomElem: => T)(
      implicit arg0: Encoder[T],
      sparkSession: SparkSession): Dataset[T] = {
    import sparkSession.implicits._

    val partitionSize =
      Math.ceil(numberOfRecords.toDouble / numberOfPartitions).toInt

    (0 until numberOfPartitions)
      .toDF("partitionId")
      .repartition(numberOfPartitions)
      .flatMap(row => {
        val partitionId         = row.getInt(0)
        val recordsPerPartition = (partitionId * partitionSize) until (partitionId + 1) * partitionSize

        recordsPerPartition.map {_ =>
          randomElem
        }.toIterator
      })
  }

  def generateRandomUdf: Seq[UserDefinedFunction] = {
    Seq(generateRandomTimestamp, generateRandomString, generateRandomInt, generateRandomCategorical)
  }

  def generateRandomTimestamp: UserDefinedFunction = {
    val offset = Timestamp.valueOf("1950-01-01 00:00:00").getTime
    val end    = Timestamp.valueOf("2010-01-01 00:00:00").getTime
    val diff   = end - offset + 1

    udf((_: Long) => new Timestamp(offset + (Math.random * diff).toLong))
  }

  def generateRandomString: UserDefinedFunction = {
    udf((_: Long) => Random.alphanumeric.take(10).mkString)
  }

  def generateRandomInt: UserDefinedFunction = udf((_: Long) => 500 + Random.nextInt(50000))

  def generateRandomCategorical: UserDefinedFunction =
    udf((_: Long) => List("M", "F")(Random.nextInt(2)))
}
