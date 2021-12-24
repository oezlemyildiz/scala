import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.types.{FloatType,DoubleType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._


object TotalAmountSpentDataSet {

  case class DataClass(custId:Int, itemId:Int,price:Double)

  def main (args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark= SparkSession
      .builder()
      .master("local[*]")
      .appName("TotalAmountDataSet")
      .getOrCreate()

    val dataSchema= new StructType()
      .add("custId",IntegerType,nullable = true)
      .add("itemId",IntegerType, nullable = true)
      .add("price",DoubleType,nullable = true)


    import spark.implicits._

    val inputData = spark.read
      .schema(dataSchema)
      .csv("data/customer-orders.csv")
      .as[DataClass]

    inputData.printSchema()
    inputData.show()
    val selectedColumn = inputData.filter($"price"> 10).select("custId","price")
    selectedColumn.show()
    val custTotalAmount= selectedColumn.groupBy("custId").agg(round(sum("price"), 2)).alias("formatlı_price")
    custTotalAmount.show()



  }


}
