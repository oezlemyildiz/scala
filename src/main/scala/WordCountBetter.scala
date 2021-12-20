import org.apache.spark._

object WordCountBetter {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(master = "local[*]", appName = "WordCountBetter")
    val input = sc.textFile("data/book.txt")
    val words = input.flatMap(x=> x.split("\\W+"))
    words.take(10).foreach(println)
    val lowerCaseWords= words.map(x=> x.toLowerCase())
    lowerCaseWords.take(5).foreach(println)
    val wordCount= lowerCaseWords.countByValue()
    wordCount.foreach(println)


  }

}
