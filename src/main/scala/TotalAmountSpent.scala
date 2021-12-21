import org.apache.spark._


object TotalAmountSpent {
  def parseLine(line: String): (Int, Float) = {
    val fields = line.split(",")
    val customerId = fields(0).toInt
    val amountSpent = fields(2).toFloat
    (customerId, amountSpent)
  }


  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(master = "local[*]", appName = "TotalAmountSpent")
    val lines = sc.textFile("data/customer-orders.csv")
    val parsedLines = lines.map(parseLine)
    println("yazılıyor")
    parsedLines.take(5).foreach(println)
    val totalAmountSpent= parsedLines.reduceByKey((x,y)=> (x+y))
    val flipped= totalAmountSpent.map(x=> (x._2,x._1)).sortByKey()
   // val totalAmountSpentCustSorted= flipped.sortByKey()
    val results= flipped.collect()
    results.take(10).foreach(println)
    for (result <-results){
      val cid=result._2
      val total=result._1
      println("   for yazılıyor")
      print(s"$cid : $total")
    }
  }
}
