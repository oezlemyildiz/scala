import org.apache.spark._
import org.apache.spark.sql.SparkSession
object read {
  def main(args:Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
  val parquet = spark.read.parquet("/Users/ozlemyildiz/Downloads/userdata1.parquet");
  parquet.show(20)

  }
}
