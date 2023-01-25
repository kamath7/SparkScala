package sample

import org.apache.log4j._
import org.apache.spark._

object HelloWorld {

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "HelloThere")

    val lines = sc.textFile("data/u.data")

    val numLines = lines.count()

    println("The data file you entered has "+numLines+" lines")
    sc.stop()
  }
}
