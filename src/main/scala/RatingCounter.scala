import org.apache.spark._
import org.apache.log4j._
object RatingCounter {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(master = "local[*]", appName = "RatingCounter")
    val data = sc.textFile("data/ml-100k/u.data")
    val ratings = data.map(x => x.split("\t")(2))
    ratings.take(20).foreach(println)
    println("take 5 ratings")
    val results = ratings.countByValue() // unique values
    val sortedResults = results.toSeq.sortBy(_._1)
    sortedResults.foreach(println)

  }

}
