import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, col, desc, udf}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}


object MostPopularSuperHeroDataSet {

  case class SuperHeroIdClass(movieId: Int)

  case class SuperHeroNameClass(movieId: Int, movieName: String)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("MostPopularSuperHero")
      .master("local[*]")
      .getOrCreate()


    val MovieIdSchema = new StructType()
      .add("movieId", IntegerType, nullable = true)

    val MovieNameSchema = new StructType()
      .add("movieId", IntegerType, nullable = true)
      .add("movieName", StringType, nullable = true)
    import spark.implicits._
    val movieNameDS = spark.read
      .schema(MovieNameSchema)
      .option("sep", " ")
      .csv("data/Marvel-names.txt")
      .as[SuperHeroNameClass]

    val movieIdDS = spark.read
      .schema(MovieIdSchema)
      .option("sep", " ")
      .csv("data/Marvel-graph.txt")
      .as[SuperHeroIdClass]

    // val aa= movieNameDS.join(movieIdDS, movieIdDS("movieId") === movieNameDS("movieId"), "inner")
    //aa.show()
    println("*****")
    val mostPopularMovieCount = movieIdDS.groupBy("movieId").count().alias("Count")

    val mostpopularMovie = movieNameDS.join(mostPopularMovieCount, mostPopularMovieCount("movieId") === movieNameDS("movieId"), "inner")
    mostpopularMovie.show()

    val cc = mostpopularMovie.groupBy( "movieName").sum("count").alias("sum")
    val sorted= mostpopularMovie.sort("count")

    sorted.show(sorted.count.toInt)

    println("bu yazma işlemi bitti")




    //val mostPopularName2= movieNameDS.filter( $"movieId" === mostPopularMovie())

  }

}