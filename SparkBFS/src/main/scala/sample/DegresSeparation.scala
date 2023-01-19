package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator

object DegresSeparation {

  val startCharacterID = 5306 //Spidey
  val targetCharacterID = 14 //Some random dude

  val hitCounter:Option[LongAccumulator] = None


  type BFSData = (Array[Int], Int, String)
  type BFSNOde = (Int, BFSData)

  def convertToBfs(line: String ) : BFSNOde= {}

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "SuperHeroMeeter")
  }

}
