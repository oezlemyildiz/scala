import org.apache.spark._

object WordCountSorted {

  def main(args:Array[String]): Unit ={

    val sc = new SparkContext(master = "local[*]", appName = "WordCountSorted")
    val input = sc.textFile("data/book.txt")
    val words = input.flatMap(x=> x.split("\\W+"))
    val lowerCaseWords= words.map(x=> x.toLowerCase())
    val wordCount= lowerCaseWords.map(x=> (x, 1)).reduceByKey((x,y)=>x+y)
    wordCount.take(10).foreach(println)
    val wordCountSorted = wordCount.map(x=> (x._2, x._1)).sortByKey()
    wordCountSorted.take(5).foreach(println)
    for(result <- wordCountSorted){
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}
