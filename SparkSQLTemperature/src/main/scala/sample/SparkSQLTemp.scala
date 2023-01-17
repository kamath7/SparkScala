package sample

import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object SparkSQLTemp {

  case class Temperature(stationID: String, date: Int, measure_type: String, temperature: Float)

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("WeatherApp").master("local[*]").getOrCreate()

    val temperatureSchema = new StructType().add("stationID", StringType, nullable = true).add("date", IntegerType, nullable = true).add("measure_type", StringType, nullable = true).add("temperature", FloatType, nullable = true)

    import spark.implicits._

    val ds = spark.read.schema(temperatureSchema).csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\1800.csv").as[Temperature]

    val minTemps = ds.filter($"measure_type" === "TMIN")

    val stationTemps = minTemps.select("stationID", "temperature")

    val minTempsByStations = stationTemps.groupBy("stationID").min("temperature")

    val results = minTempsByStations.collect()

    for (result <- results){
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f C"

      println(s"$station min temp : $formattedTemp")
    }
  }
}
