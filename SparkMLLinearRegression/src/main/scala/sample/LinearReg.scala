package sample

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession

"D:\\Code\\Scala\\SparkAndScala\\Datasets\\regression.text"
object LinearReg {

  case class RegressionSchema(label:Double, features_raw: Double)
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("LinearReg")
      .master("local[*]")
      .getOrCreate()




    spark.stop()
  }
}
