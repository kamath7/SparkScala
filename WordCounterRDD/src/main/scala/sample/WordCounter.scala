package sample

import org.apache.log4j._
import org.apache.spark._

object WordCounter {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","WordCounter")

    val inp = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\book.txt")

    val words = inp.flatMap(x => x.split("\\W+")) //get individual words


    val lowerCaseWords = words.map(x => x.toString().toLowerCase())
    val wordCounts = lowerCaseWords.map(x => (x,1)).reduceByKey((x,y) => x+y)

    val wordCountsSorted = wordCounts.map(x => (x._2, x._1)).sortByKey()

    for (result <- wordCountsSorted){
      println(s"${result._1} -> ${result._2}")
    }

//    val wordCount = lowerCaseWords.countByValue()
//
//    val sortedWordCount = wordCount.toSeq.sortBy(_._2)
//
//    sortedWordCount.foreach(println)
  }
}
