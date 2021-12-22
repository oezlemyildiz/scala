import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.{types, _}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._


object MinTemperatureDataSet {
  case class Temperature(stationId: String, date: Int, measure_type: String, temperature: Float)

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("MinTempatureDS")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationId", StringType, nullable=true)
      .add("date",IntegerType,nullable=true )
      .add("measure_type",StringType,nullable = true)
      .add("temperature",FloatType, nullable = true)

    import spark.implicits._
    val temperatureDS = spark.read
      .schema(temperatureSchema)
      //.option("inferSchema", "true")
      .csv("data/1800.csv")
      .as[Temperature]

    temperatureDS.printSchema()

    val filteredTemperatureDS = temperatureDS.filter( $"measure_type"==="TMIN")
    val stationTempsDS= filteredTemperatureDS.select("stationId","temperature")
    val minTemperatureDS = stationTempsDS.groupBy( "stationId").min("temperature")

    minTemperatureDS.show()

    val minTempsByStationF = minTemperatureDS
      .withColumn("temperature", round($"min(temperature)" * 0.1f * (9.0f / 5.0f) + 32.0f, 2))
      .select("stationId", "temperature").sort("temperature")
    minTempsByStationF.show()

    // Collect, format, and print the results
    val results = minTempsByStationF.collect()

    for (result <- results) {
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }
    spark.stop()
  }
}
