package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer

object DegresSeparation {

  val startCharacterID = 5306 //Spidey
  val targetCharacterID = 14 //Some random dude

  val hitCounter:Option[LongAccumulator] = None


  type BFSData = (Array[Int], Int, String)
  type BFSNOde = (Int, BFSData)

  def convertToBfs(line: String ) : BFSNOde= {

    val fields = line.split("\\s+")
    val heroID = fields(0).toInt

    val connections: ArrayBuffer[Int] = ArrayBuffer()

    for (connection <- 1 until(fields.length - 1)){
      connections += fields(connection).toInt
    }

    var color: String = "WHITE"
    var distance:Int = 9999

    if (heroID == startCharacterID){
      color = "GRAY"
      distance = 0
    }
    (heroID, (connections.toArray, distance, color))
  }

  def createStartingRdd(sc: SparkContext):RDD[BFSNOde] = {
    val inputFile = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\Marvel-graph.txt")
    inputFile.map(convertToBfs)
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "SuperHeroMeeter")

    hitCounter = Some(sc.longAccumulator())
  }

}
