import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}
import org.apache.spark.sql.functions._

object PopularMoviesDataSet {

  case class DataClass( userId:Int, movieId:Int, rating:Int, timestamp:Long)

  def main (args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MovieDataSet")
      .master("local[*]")
      .getOrCreate()


    val dataSchema= new StructType()
      .add("userId",  IntegerType,nullable = true)
      .add("movieId", IntegerType,nullable = true)
      .add("rating",  IntegerType,nullable = true)
      .add("timestamp",LongType,  nullable = true)

    import spark.implicits._
   val inputData= spark.read
     .schema(dataSchema)
     .option("sep","\t")
     .csv("data/ml-100k/u.data")
     .as[DataClass]


    inputData.show()
    val countDS=inputData.groupBy("movieId").count().orderBy(desc("count"))
    countDS.show(countDS.count.toInt)

  }

}
