import org.apache.spark._

import scala.math._


object MinTemperature {

  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temperature)
  }

  def main(args: Array[String])={

    val sc= new SparkContext(master = "local[*]", appName = "MinTemperature")
    val lines= sc.textFile("data/1800.csv")
    lines.foreach(println)
    // (stationId, entryType, temperature)
    val parsedlines = lines.map(parseLine)
    parsedlines.take(5).foreach(println)
    val minTemps =parsedlines.filter(x=> x._2=="TMIN")
    val maxTemps =parsedlines.filter(x=> x._2=="TMAX")
    minTemps.take(5).foreach(println)
    val stationTemps= minTemps.map(x=> (x._1, x._3.toFloat))
    val maxStationTemps= maxTemps.map(x=> (x._1,x._3.toFloat))
    stationTemps.take(5).foreach(println)
    val minTemperatureByStation= stationTemps.reduceByKey( (x,y) => min(x,y))
    val maxTemperatureByStation= maxStationTemps.reduceByKey( (x,y) => max(x,y))
    val results = minTemperatureByStation.collect()
    val maxresults= maxTemperatureByStation.collect()
    for (result <- results.sorted){
      val station=result._1
      val temp= result._2
      val formattedTemp= f"$temp%.2f F"
      println(s"$station minimum temperature : $formattedTemp" )
    }
      for (result <- maxresults.sorted){
        val station=result._1
        val temp= result._2
        val formattedTemp= f"$temp%.2f F"
        println(s"$station maximum temperature : $formattedTemp" )
    }




  }

}
