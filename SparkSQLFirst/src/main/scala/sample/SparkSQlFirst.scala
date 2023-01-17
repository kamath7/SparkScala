package sample

import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object SparkSQlFirst {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("SparkSQLfirst").master("local[*]").getOrCreate()

    import spark.implicits._

    val peopleSchema = spark.read.option("header", "true").option("inferSchema", "true").csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\fakefriends.csv").as[Person]

    peopleSchema.printSchema()

    peopleSchema.createOrReplaceTempView("people")

    val teenagers = spark.sql("SELECT * FROM people WHERE age >=13 AND age <= 19")
    val res = teenagers.collect()
    res.foreach(println)

    spark.stop()

  }
}
