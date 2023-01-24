package sample

import org.apache.log4j._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StructType}



object LinearReg {

  case class RegressionSchema(label:Double, features_raw: Double)
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearReg")
      .master("local[*]")
      .getOrCreate()

    val regressionSchema = new StructType()
      .add("label",DoubleType, nullable = true)
      .add("features_raw", DoubleType, nullable = true)

    import spark.implicits._

    val dsRaw = spark.read
      .option("sep",",")
      .schema(regressionSchema)
      .csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\regression.txt")
      .as[RegressionSchema]

    val assembler = new VectorAssembler().setInputCols(Array("features_raw")).setOutputCol("features")

    val df = assembler.transform(dsRaw).select("label","features")

    val trainTest = df.randomSplit(Array(0.5, 0.5))
    val trainingDF = trainTest(0)
    val testDF = trainTest(1)

    val lir = new LinearRegression()
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(100)
      .setTol(1E-6)

    val model = lir.fit(trainingDF)

    val fullPreds = model.transform(testDF).cache()

    val predictionAndLabel = fullPreds.select("prediction", "label").collect()

    for(prediction <- predictionAndLabel){
      println(prediction)
    }

    spark.stop()
  }
}
