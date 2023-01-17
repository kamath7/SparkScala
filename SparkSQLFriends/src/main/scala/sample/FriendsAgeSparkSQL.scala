package sample

import breeze.numerics.round
import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object FriendsAgeSparkSQL {

  case class FakeFriends(id: Int, name: String, age: Int, friends: Long)

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("FriendsByAge").master("local[*]").getOrCreate()

    import spark.implicits._

    val ds = spark.read.option("header","true").option("inferSchema","true").csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\fakefriends.csv").as[FakeFriends]

    val friendsByAge = ds.select("age","friends")

    friendsByAge.groupBy("age").avg("friends").show()

    friendsByAge.groupBy("age").avg("friends").sort("age").show()
  }
}
