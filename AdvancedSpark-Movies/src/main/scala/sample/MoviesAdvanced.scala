package sample

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

object MoviesAdvanced {

  final case class Movie(movieID: Int)

  def main(args: Array[String]) = {

   Logger.getLogger("org").setLevel(Level.ERROR)

   val spark = SparkSession.builder.appName("Movies").master("local[*]").getOrCreate()

    val movieSchema = new StructType()
      .add("userID", IntegerType, nullable = true)
      .add("movieID", IntegerType, nullable = true)
      .add("rating", IntegerType, nullable = true)
      .add("timestamp", LongType, nullable = true)

    import spark.implicits._

    val moviesDB = spark.read.option("sep","\t").schema(movieSchema).csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\ml-100k\\u.data").as[Movie]

    val topMovieIds = moviesDB.groupBy("movieID").count().orderBy(desc("count"))

    topMovieIds.show(10)

    spark.stop()
  }
}
