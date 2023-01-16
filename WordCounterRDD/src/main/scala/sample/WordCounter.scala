package sample

import org.apache.log4j._
import org.apache.spark._

object WordCounter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","WordCounter")

    val inp = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\book.txt")

    val words = inp.flatMap(x => x.split(" ")) //get individual words

    val wordCount = words.countByValue()

    wordCount.foreach(println)
  }
}