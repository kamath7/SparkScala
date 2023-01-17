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

  }
}
