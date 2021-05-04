import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession

case class DataLR(population: Double, profit: Double)
object LinearRegression extends App{

  val spark = SparkSession
    .builder()
    .appName("LR")
    .master("local[*]")
    .getOrCreate()

  val path = "C:\\Users\\RaffaeleRomeo\\MyWorkspace\\learning\\machine-learning\\src\\main\\resources\\ex1data1LR.csv"
  import spark.implicits._

  val restaurantFranchise = spark.sparkContext.textFile(path, 6)
    .filter(str => str != "population,profit")
    .map( x => Vectors.dense(x.split(",").map(_.trim().toDouble)))

  //Analyzing the data
  import org.apache.spark.mllib.linalg.distributed.RowMatrix
  val restaurantFranchiseMat = new RowMatrix(restaurantFranchise)
  val restaurantFranchiseStats = restaurantFranchiseMat.computeColumnSummaryStatistics()

  import org.apache.spark.mllib.stat.Statistics
  val restaurantFranchiseStats2 = Statistics.colStats(restaurantFranchise)

  val restaurantFranchiseColSims = restaurantFranchiseMat.columnSimilarities()

  //Data preparation..Splitting the data
  import org.apache.spark.mllib.regression.LabeledPoint
  val restaurantFranchiseData = restaurantFranchise.map(x => {
    val a = x.toArray
    LabeledPoint(a(a.length-1), Vectors.dense(a.slice(0, a.length-1)))
  })

  val sets = restaurantFranchiseData.randomSplit(Array(0.8, 0.2))
  val restaurantFranchiseTrain = sets(0)
  val restaurantFranchiseValid = sets(1)

  //Feature scaling
  import org.apache.spark.mllib.feature.StandardScaler
  val scaler = new StandardScaler(true, true).fit(restaurantFranchiseTrain.map(x => x.features))

  val trainScaled = restaurantFranchiseTrain.map(x => LabeledPoint(x.label, scaler.transform(x.features)))
  val validScaled = restaurantFranchiseValid.map(x => LabeledPoint(x.label, scaler.transform(x.features)))

  //Linear regression
  import org.apache.spark.ml.regression.LinearRegression
  val lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)

  // Fit the model
  val lrModel = lr.fit(trainScaled.toDF())

  import org.apache.spark.ml.linalg.Vectors
  restaurantFranchiseValid.map(x => (lrModel.predict(Vectors.dense(x.features.toArray)), x.label))
    .foreach(println)





}


