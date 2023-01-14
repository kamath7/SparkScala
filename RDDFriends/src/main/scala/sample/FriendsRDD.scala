package sample

import org.apache.log4j._
import org.apache.spark._
object FriendsRDD {

  //The dataset will be looking as follows

  //ID, NAME, AGE, NUM_OF_FRIENDS
  /*
  1, Adithya, 26, 1
  2, ShreeHari, 34, 299
   */

  def parseMyLines(line: String): (Int, Int) = {
    val fields = line.split(",")

    val age = fields(2).toInt
    val numOfFriends = fields(3).toInt

    (age, numOfFriends)
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsRDD")

    val lines = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\fakefriends-noheader.csv")

    val rdd = lines.map(parseMyLines)


    val totalByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))

    val avgsByAge = totalByAge.mapValues(x => x._1/x._2)

    val results = avgsByAge.collect()

    results.sorted.foreach(println)
  }


}
