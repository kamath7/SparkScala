package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object AnotherImplementation {
  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("SparkSQLBetter").master("local[*]").getOrCreate()

    import spark.implicits._
    val people = spark.read
                      .option("header","true")
                      .option("inferSchema","true")
                      .csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\fakefriends.csv").as[Person]

    people.printSchema()


    people.select("name").show()

    println("------------------")

    people.filter(people("age") < 21).show()

    println(" ----------------- ")

    people.groupBy("age").count().show()

    println(" ----------------- ")

    people.select(people("name"), people("age") + 10).show()

    spark.stop()
  }
}
