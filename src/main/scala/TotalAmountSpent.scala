import org.apache.spark._


object TotalAmountSpent {

  def main (args : Array[String]): Unit ={
    val sc = new SparkContext(master = "local[*]", appName = "TotalAmountSpent")
    val lines = sc.textFile("data/customer-order.csv")
    val parsedLines = lines.map(x=> x.split(","))
    parsedLines.foreach(println)
  }
}
