import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum,round}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}

object SparkSQLCustOrders {

  case class CustOrders(cust_id: Int, item_id: Int, amount_spent: Double)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("TotalSpentCust").master("local[*]").getOrCreate()

    val custOrdersSchema = new StructType().add("cust_id", IntegerType, nullable = true).add("item_id",IntegerType,nullable = true).add("amount_spent",DoubleType, nullable = true)


    import spark.implicits._

    val customerDS = spark.read.schema(custOrdersSchema).csv("D:\\Code\\Scala\\SparkAndScala\\Datasets\\customer-orders.csv").as[CustOrders]

   val totalByCust = customerDS.groupBy("cust_id").agg(round(sum("amount_spent"),2).alias("Total_spent"))
    val totalCustSorted = totalByCust.sort("Total_spent")

    totalCustSorted.show(totalByCust.count.toInt)
  }
}
