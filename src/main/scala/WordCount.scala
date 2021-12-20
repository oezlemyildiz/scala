import org.apache.spark._

object WordCount {
  def main(args: Array[String] ): Unit = {

    val sc = new SparkContext(master = "local[*]", appName = "WordCount")
    val input = sc.textFile("data/book.txt")
    val words = input.flatMap(x => x.split(" "))
    val wordCount = words.countByValue()
    wordCount.foreach(println)
  }
}
