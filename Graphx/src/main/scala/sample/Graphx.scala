package sample

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.graphx._

object Graphx {

  def parseNames(line: String): Option[(VertexId, String)] = {
    val fields = line.split('\"')
    if (fields.length > 1){
      val heroID: Long = fields(0).trim().toLong
      if (heroID < 6487) //last valid characted
      {
        return Some(fields(0).trim().toLong, fields(1))
      }
    }
    None
  }
  def makeEdges(line: String) : List[Edge[Int]] = {
    import scala.collection.mutable.ListBuffer
    var edges = new ListBuffer[Edge[Int]]()
    val fields = line.split(" ")
    val origin = fields(0)
    for (x <- 1 until (fields.length - 1)) {
      // Our attribute field is unused, but in other graphs could
      // be used to deep track of physical distances etc.
      edges += Edge(origin.toLong, fields(x).toLong, 0)
    }

    edges.toList
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "GraphX")

    val names = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\Marvel-names.txt")

    val verts = names.flatMap(parseNames)

    val lines = sc.textFile("D:\\Code\\Scala\\SparkAndScala\\Datasets\\Marvel-graph.txt")
    val edges = lines.flatMap(makeEdges)

    val default = "Nobody"
    val graph = Graph(verts, edges, default).cache()

    println("Most connected superheroes")
    graph.degrees.join(verts).sortBy(_._2._1, ascending=false).take(10).foreach(println)

    val root: VertexId = 5306 //Spiderman ID

    val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)


    val bfs = initialGraph.pregel(Double.PositiveInfinity, 10)(

      (id, attr, msg) => math.min(attr, msg),
      triplet => {
        if (triplet.srcAttr != Double.PositiveInfinity) {
          Iterator((triplet.dstId, triplet.srcAttr + 1))
        } else {
          Iterator.empty
        }
      }, (a, b) => math.min(a, b)).cache()

    bfs.vertices.join(verts).take(100).foreach(println)

    println("\n\nDegrees from SpiderMan to ADAM 3,031")
    bfs.vertices.filter(x => x._1 == 14).collect.foreach(println)
  }
}
