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


  def computeCosineSimilarity(spark:SparkSession, data: Dataset[MoviePairs]): Dataset[MoviePairsSimilarity] = {

    val pairScores = data
      .withColumn("xx", col("rating1") * col("rating2"))
      .withColumn("yy", col("rating2") * col("rating2"))
      .withColumn("xy", col("rating1") * col("rating2"))

    val calculateSimilarity = pairScores
      .groupBy("movie1","movie2")
      .agg(
        sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx"))) * sqrt(sum(col("yy")))).alias("denominator"),
      count(col("xy")).alias("numPairs")
      )

    import spark.implicits._

    val result = calculateSimilarity
      .withColumn("score",
        when(col("denominator") =!= 0, col("numerator") / col ("denominator"))
      .otherwise(null)).select("movie1", "movie2","score","numPairs").as[MoviePairsSimilarity]

    result
  }

  def getMovieName(movieNames: Dataset[MoviesNames], movieId: Int): String = {
    val result = movieNames.filter(col("movieID") === movieId)
      .select("movieTitle").collect()(0)

    result(0).toString
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder.appName("RecommenderEngine")
      .master("local[*]")
      .getOrCreate()

    val moviesNameSchema = new StructType()
      .add("movieID", IntegerType, nullable = true)
      .add("movieTitle",StringType, nullable = true)

    val movieSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    println("movie names go here 👇")
    import spark.implicits._

    val movieNames = spark.read
      .option("sep","|")
      .option("charset","ISO-8859-1")
      .schema(moviesNameSchema)
      .csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.item")
      .as[MoviesNames]

    val movies = spark.read
      .option("sep","\t")
      .schema(movieSchema)
      .csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.data")
      .as[Movies]

    val ratings = movies.select("userID", "movieID","rating")

    val moviePairs = ratings.as("ratings1")
      .join(ratings.as("ratings2"), $"ratings1.userID" === $"ratings2.userID" && $"ratings1.movieID" < $"ratings2.movieID")
      .select($"ratings1.movieID".alias("movie1"),
        $"ratings2.movieID".alias("movie2"),
        $"ratings1.rating".alias("rating1"),
        $"ratings2.rating".alias("rating2")
      ).repartition(100).as[MoviePairs]

    val moviePairsSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

    if(args.length > 0){
      val scoreThreshold = 0.97
      val coOccurenceThreshold = 50.0

      val movieID: Int = args(0).toInt

      val filterResults = moviePairsSimilarities.filter(
        (col("movie1") === movieID || col("movie2") === movieID) &&
          col("score") > scoreThreshold && col("numPairs") > coOccurenceThreshold
      )

      val results = filterResults.sort(col("score").desc).take(10)

      println("Top 10 similar movies for "+getMovieName(movieNames, movieID))

      for (result <- results){

        var similarMovieID = result.movie1
        if (similarMovieID == movieID) {
          similarMovieID = result.movie2
        }

        println(getMovieName(movieNames, similarMovieID) + "\t Score -> "+result.score+" \t Strength -> "+result.numPairs)
      }
    }
  }
}
