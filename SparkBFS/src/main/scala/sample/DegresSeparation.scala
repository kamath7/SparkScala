package sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
import scala.sys.exit

object DegresSeparation {

  val startCharacterID = 5306 //Spidey
  val targetCharacterID = 14 //Some random dude

  var hitCounter: Option[LongAccumulator] = None


  type BFSData = (Array[Int], Int, String)
  type BFSNode = (Int, BFSData)

  def convertToBfs(line: String ) : BFSNode= {

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

  def createStartingRdd(sc: SparkContext):RDD[BFSNode] = {
    val inputFile = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\Marvel-graph.txt")
    inputFile.map(convertToBfs)
  }

  def bfsMap(node:BFSNode): Array[BFSNode] = {

    // Extract data from the BFSNode
    val characterID:Int = node._1
    val data:BFSData = node._2

    val connections:Array[Int] = data._1
    val distance:Int = data._2
    var color:String = data._3

    // This is called from flatMap, so we return an array
    // of potentially many BFSNodes to add to our new RDD
    var results:ArrayBuffer[BFSNode] = ArrayBuffer()

    // Gray nodes are flagged for expansion, and create new
    // gray nodes for each connection
    if (color == "GRAY") {
      for (connection <- connections) {
        val newCharacterID = connection
        val newDistance = distance + 1
        val newColor = "GRAY"

        // Have we stumbled across the character we're looking for?
        // If so increment our accumulator so the driver script knows.
        if (targetCharacterID == connection) {
          if (hitCounter.isDefined) {
            hitCounter.get.add(1)
          }
        }

        // Create our new Gray node for this connection and add it to the results
        val newEntry:BFSNode = (newCharacterID, (Array(), newDistance, newColor))
        results += newEntry
      }

      // Color this node as black, indicating it has been processed already.
      color = "BLACK"
    }

    // Add the original node back in, so its connections can get merged with
    // the gray nodes in the reducer.
    val thisEntry:BFSNode = (characterID, (connections, distance, color))
    results += thisEntry

    results.toArray
  }

  def bfsReduce(data1:BFSData, data2:BFSData): BFSData = {

    // Extract data that we are combining
    val edges1:Array[Int] = data1._1
    val edges2:Array[Int] = data2._1
    val distance1:Int = data1._2
    val distance2:Int = data2._2
    val color1:String = data1._3
    val color2:String = data2._3

    // Default node values
    var distance:Int = 9999
    var color:String = "WHITE"
    var edges:ArrayBuffer[Int] = ArrayBuffer()

    // See if one is the original node with its connections.
    // If so preserve them.
    if (edges1.length > 0) {
      edges ++= edges1
    }
    if (edges2.length > 0) {
      edges ++= edges2
    }

    // Preserve minimum distance
    if (distance1 < distance) {
      distance = distance1
    }
    if (distance2 < distance) {
      distance = distance2
    }

    // Preserve darkest color
    if (color1 == "WHITE" && (color2 == "GRAY" || color2 == "BLACK")) {
      color = color2
    }
    if (color1 == "GRAY" && color2 == "BLACK") {
      color = color2
    }
    if (color2 == "WHITE" && (color1 == "GRAY" || color1 == "BLACK")) {
      color = color1
    }
    if (color2 == "GRAY" && color1 == "BLACK") {
      color = color1
    }
    if (color1 == "GRAY" && color2 == "GRAY") {
      color = color1
    }
    if (color1 == "BLACK" && color2 == "BLACK") {
      color = color1
    }

    (edges.toArray, distance, color)
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "SuperHeroMeeter")

    hitCounter = Some(sc.longAccumulator("Hit Counter"))

    var iterationRdd = createStartingRdd(sc)

    for (iteration <- 1 to 10) {
      println("Running BFS Iteration# " + iteration)

      // Create new vertices as needed to darken or reduce distances in the
      // reduce stage. If we encounter the node we're looking for as a GRAY
      // node, increment our accumulator to signal that we're done.
      val mapped = iterationRdd.flatMap(bfsMap)

      // Note that mapped.count() action here forces the RDD to be evaluated, and
      // that's the only reason our accumulator is actually updated.
      println("Processing " + mapped.count() + " values.")

      if (hitCounter.isDefined) {
        val hitCount = hitCounter.get.value
        if (hitCount > 0) {
          println("Hit the target character! From " + hitCount +
            " different direction(s).")
          exit
        }
      }

      // Reducer combines data for each character ID, preserving the darkest
      // color and shortest path.
      iterationRdd = mapped.reduceByKey(bfsReduce)
    }
  }

}
