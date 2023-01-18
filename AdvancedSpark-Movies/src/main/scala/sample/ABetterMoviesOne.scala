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

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("PopMovies").master("local[*]").getOrCreate()

    val nameDict = spark.sparkContext.broadcast(loadMovieNames())

    val movieSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._
    val moviesDB = spark.read.option("sep","\t").schema(movieSchema).csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.data").as[Movies]

    val topMovieIds = moviesDB.groupBy("movieID").count()

    val lookUpName: Int => String = (movieID: Int) => {
      nameDict.value(movieID)
    }

    val lookUpNameUDF = udf(lookUpName)

    val movieWithNames = topMovieIds.withColumn("MovieTitle",lookUpNameUDF(col("movieID")))

    val sortedMovieNames = movieWithNames.sort("count")

    sortedMovieNames.show(sortedMovieNames.count.toInt, truncate = false)
  }
}
