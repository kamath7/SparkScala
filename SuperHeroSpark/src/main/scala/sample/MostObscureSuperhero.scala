package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.functions.{col, min, size, split, sum}

object MostObscureSuperhero {
  case class SuperHeroNames(id: Int ,name: String)
  case class Superhero (value: String)
  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("Obscuresuperhero").master("local[*]").getOrCreate()

    val SuperHeroSchema = new StructType().add("id",IntegerType, nullable = true).add("name", StringType, nullable = true)

    import spark.implicits._

    val names = spark.read.schema(SuperHeroSchema).option("sep"," ").csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\Marvel-names.txt").as[SuperHeroNames]
    val lines = spark.read.text("D:\\Code\\Scala\\SparkAndScala\\Datasets\\Marvel-graph.txt").as[Superhero]

    val connections = lines
      .withColumn("id", split(col("value"), " ")(0))
      .withColumn("connections", size(split(col("value"), " ")) - 1)
      .groupBy("id").agg(sum("connections").alias("connections"))

    val minConnectionsCount = connections.agg(min("connections")).first().getLong(0)

    val minConnections = connections.filter($"connections" === minConnectionsCount)

    val minConnectionsWithNames = minConnections.join(names, usingColumn = "id")

    print("Following characters have the least appearance "+minConnectionsCount)

    minConnectionsWithNames.select("name").show()

  }
}
