package sample

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.sql._

object RegressionRealEstate {

  case class RegressionSchema(No:Integer, TransactionDate: Double, HouseAge: Double, DistanceToMRT: Double, NumberConvenienceStores: Integer, Latitude: Double, Longitude: Double, PriceOfUnitArea: Double)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("RegressionHouse")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._
    val dsRaw = spark.read
      .option("sep",",")
      .option("header","true")
      .option("inferSchema","true")
      .csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\realestate.csv")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().setInputCols(Array("HouseAge","DistanceToMRT","NumberConvenienceStores")).setOutputCol("features")

    val df = assembler.transform(dsRaw).select("PriceOfUnitArea","features")

    val trainTest = df.randomSplit(Array(0.5,0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    val dtr = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("PriceOfUnitArea")


    val model = dtr.fit(trainingDF)

    val fullPreds = model.transform(testDF).cache()

    val predLabel = fullPreds.select("prediction", "PriceOfUnitArea").collect()

    for (prediction <- predLabel){
      println(prediction)
    }

    spark.stop()
  }
}
