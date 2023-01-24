package sample

import org.apache.log4j._
import org.apache.spark.ml.recommendation._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType}

object RecommenderEngineML {
  case class MoviesNames(movieId: Int, movieTitle: String)
  case class Rating(userID:Int, movieID: Int, rating: Float)

  def getMovieName(moviesNames: Array[MoviesNames], movieId:Int): String = {
    val result = moviesNames.filter(_.movieId == movieId)(0)
    result.movieTitle
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("MovieRecommender").master("local[*]").getOrCreate()

    println("Movie names..")

    val moviesNamesSchema = new StructType().add("movieId", IntegerType, nullable = true).add("movieTitle", StringType, nullable = true)

    val moviesSchema = new StructType().add("userID", IntegerType, nullable = true).add("movieID", IntegerType, nullable = true).add("rating", IntegerType, nullable=true).add("timestamp",LongType, nullable = true)

    import spark.implicits._

    val names = spark.read
      .option("sep","|")
      .option("charset","ISO-8859-1")
      .schema(moviesNamesSchema)
      .csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.item")
      .as[MoviesNames]

    val namesList = names.collect()

    val ratings = spark.read
      .option("sep","\t")
      .schema(moviesSchema)
      .csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.data")
      .as[Rating]


  }
}
