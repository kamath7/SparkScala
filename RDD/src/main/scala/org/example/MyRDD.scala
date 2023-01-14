package org.example

import org.apache.spark._
import org.apache.log4j._

object MyRDD {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","Ratings")

    val lines = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.data")



  }
}

