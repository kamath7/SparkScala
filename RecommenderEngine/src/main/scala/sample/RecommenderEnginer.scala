package sample

import org.apache.log4j._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType,StructType,LongType}

object RecommenderEnginer {

  case class Movies(userID:Int, movieID:Int, rating: Int, timestamp:Long)
  case class MoviesNames(movieID:Int, movieTitle: String)
  case class MoviePairs(movie1: Int, movie2 :Int, rating1: Int, rating2: Int)
  case class MoviePairsSimilarity(movie1: Int, movie2: Int, score: Double, numPairs: Long)


}
