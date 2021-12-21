import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._



object WordCountDataSet {
  case class Book(value:String)

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCounter")
      .getOrCreate()

    import spark.implicits._
    val data=spark.read.text("data/book.txt").as[Book]
    val words= data
      .select(explode(split($"value","\\W+")).alias("word"))
      .filter($"word" =!= "")

    val lowerWords=words.select(lower($"word").alias("word"))
    println("wordCounts1 sort")
    val wordCounts1= lowerWords.groupBy("word").count().sort().show()
    val wordCounts= lowerWords.groupBy("word").count()
    println("sortedWordCounts1 show")
    val sortedWordCounts1=wordCounts.sort("count").show()
    val sortedWordCounts=wordCounts.sort("count")
    sortedWordCounts.show(sortedWordCounts.count.toInt)

    // RDD is better for unstructured data

    val bookRDD= spark.sparkContext.textFile("data/book.txt")
    val wordsRDD=bookRDD.flatMap(x=> x.split("\\W+"))
    val wordsDS = wordsRDD.toDS()
    wordsDS.show()
    val lowerWordsDS = wordsDS.select(lower($"value").alias("word"))
    val wordCountDS= lowerWordsDS.groupBy("word").count()
    val wordCountSortedDS=wordCountDS.sort("count")
    wordCountSortedDS.show(wordCountSortedDS.count.toInt)








  }

}
