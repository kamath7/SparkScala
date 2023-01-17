package sample

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountSparkSQL {

  case class Book(value: String)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("WordCountReader").master("local[*]").getOrCreate()

    import spark.implicits._
    val input = spark.read.text("D:\\Code\\Scala\\SparkAndScala\\Datasets\\book.txt").as[Book]

    val words = input.select(explode(split($"value","\\W+")).alias("word")).filter($"word" =!= "")

    val lowerCaseWords = words.select(lower($"word").alias("word"))

    val wordCounts = lowerCaseWords.groupBy("word").count()


  }
}
