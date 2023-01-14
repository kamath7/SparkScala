package sample

import org.apache.spark._
import org.apache.log4j._

object FirstRDD {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Ratings")

    val lines = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.data")


    //This is how the data looks

    /*
    * userID movieID rating epoch_time
    * 19 22 3 183818
    * 23 21 5 919292
    * */

    val ratings = lines.map(x => x.toString().split("\t")(2)) //converting each line of rating to string

    val results = ratings.countByValue() // gives you 3,2 5,3 . Basically how many times a particular rating ahs appeared

    val sortedRes = results.toSeq.sortBy(_._1)
    sortedRes.foreach(println)
  }
}
