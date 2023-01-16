package sample

import org.apache.log4j._
import org.apache.spark._

object CommerceRDD {

  def parseLines(line:String):(Int, Double) = {
    val field = line.split(",")
    val custId = field(0).toInt
    val amt = field(2).toFloat
    (custId, amt)
  }
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","CommerceRDD")

    val lines = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\customer-orders.csv")

    val custOrders = lines.map(parseLines)

    val custOrdersTotal = custOrders.reduceByKey((x,y) => x + y)
    custOrdersTotal.collect().foreach(println)
  }
}
