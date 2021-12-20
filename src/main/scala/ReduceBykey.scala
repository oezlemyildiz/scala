import org.apache.spark._

object ReduceBykey {
  def main(args: Array[String]) = {
    val sc = new SparkContext(master = "local[*]", appName = "ReduceBykey")

    val data = Seq(("Project", 1),
      ("Gutenberg’s", 1),
      ("Alice’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1),
      ("Adventures", 1),
      ("in", 1),
      ("Wonderland", 1),
      ("Project", 1),
      ("Gutenberg’s", 1))

    val rdd = sc.parallelize(data)
    val rdd2= sc.wholeTextFiles("data/fakefriends-noheader.csv")
    rdd2.foreach(record=>println("FileName : "+record._1+", FileContents :"+record._2))
    rdd.foreach(println)
  }
}
