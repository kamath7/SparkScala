package sample

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

import scala.io.{Codec, Source}
object ABetterMoviesOne {

  case class Movies(userID: Int, movieID: Int, rating: Int, timestamp: Long)

  def loadMovieNames() : Map[Int, String] = {

    implicit val codec: Codec = Codec("ISO-8859-1") //Codec fo the file

    var movieNames: Map[Int, String ] = Map()

    val lines = Source.fromFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.item")
    for (line <- lines.getLines()){
      val fields = line.split("|")
      if (fields.length > 1){
        movieNames += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()

    movieNames
  }
}
