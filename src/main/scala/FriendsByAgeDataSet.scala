import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FriendsByAgeDataSet {

case class Friends(id:Int, name:String, age:Int, friends:Long )

  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("friendsByAge")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val friend = spark.read
      .option("header","true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Friends]
    //friendsSchema.createOrReplaceTempView("friend")

    println("print schema")
    friend.printSchema()
    val ds_friendsByAge= friend.select("age","friends")
    ds_friendsByAge.show()
    ds_friendsByAge.groupBy("age").avg("friends").sort("age").show()
    ds_friendsByAge.groupBy("age").agg(round(avg("friends"),2)).alias("friends_avg").sort("age").show()



  }

}
