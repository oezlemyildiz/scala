import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object SparkSqlDataSet {

  case class Person(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val schemaPeople = spark.read
      .option("header", true)
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]

    schemaPeople.printSchema()
    schemaPeople.createOrReplaceTempView("people")
    import spark.implicits._
    spark.sql("select * from people").show()
    val teenagers= spark.sql("select * from people where age between 13 and 19")
    val results = teenagers.collect()
    results.foreach(println)
    spark.stop()


  }

}
