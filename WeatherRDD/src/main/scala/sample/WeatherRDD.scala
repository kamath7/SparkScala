package sample

import org.apache.log4j._
import org.apache.spark._

object WeatherRDD {
  /*
  Data is in this format

  StationID,Year,EitherTMAXorTMIN,Temperature
  9129192,18000911,TMAX,24
   */

  def parseLines(line: String): (String, String, Float)= {

    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat
    (stationID, entryType, temperature)
  }

  def celsToFahr (fahrTemp: Double): Double = ((fahrTemp - 32) * 5 )/ 9

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","WeatherRDD")

    val lines = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\1800.csv")

    val parsedLines = lines.map(parseLines)

    val minTemps = parsedLines.filter(x => x._2 == "TMIN") //check the second value in the tuple is a min
    val maxTemps = parsedLines.filter(x => x._2 == "TMAX") //check the second value if max

    val stationTemps = minTemps.map(x => (x._1, x._3))
    val stationTemps1 = maxTemps.map(x => (x._1, x._3))


    val minTempsByStation = stationTemps.reduceByKey( (x,y) => math.min(x,y) )
    val maxTempsByStations = stationTemps1.reduceByKey( (x,y) => math.max(x,y))

    val results = minTempsByStation.collect()
    for (result <- results.sorted){
      val station = result._1
      val temp = result._2

      println(s"${station} -> ${celsToFahr(temp)}")
    }

    val results1 = maxTempsByStations.collect()
    for (result <- results1.sorted){
      val station = result._1
      val temp = result._2

      println(s"${station} -> ${celsToFahr(temp)}")
    }
  }
}
