
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object DataFrameDataSet {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    println("Here is inferred schema:")
    people.printSchema()

    println("Let's select the name column")
    people.select("name").show()

    println("filter operation")
    people.filter(people("age") < 21).show()

    println("group by")
    people.groupBy(people("age")).count().show()

    println("+10 older age")
    people.select(people("age"), people("age") + 10).show()
    spark.stop()
  }
}






