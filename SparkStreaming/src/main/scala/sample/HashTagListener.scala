package sample

import org.apache.log4j._

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

object HashTagListener {

  def setupLogger(): Unit = {
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
  }
  def setUpTwitter():Unit = {
    import scala.io.Source

    val lines = Source.fromFile("D:\\Code\\Scala\\SparkAndScala\\twitter.txt")

    for (line <- lines.getLines){
      val fields = line.split(" ")
    }
  }

  def main(args: Array[String]): Unit = {
    setUpTwitter()

    val ssc = new StreamingContext("local[*]", "PopHashTags",Seconds(2))

    setupLogger()

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(status => status.getText)

    val tweetwords = statuses.flatMap(tweetText => tweetText.split(" "))

    val hashtags = tweetwords.filter(word => word.startsWith("#"))

    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow( (x,y) => x + y, (x,y) => x - y, Seconds(300), Seconds(1))


    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))


    sortedResults.print

    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }
}
