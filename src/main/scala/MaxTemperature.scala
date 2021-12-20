import org.apache.spark._
import scala.math._


object MaxTemperature {

  def parseLine(line: String) = {
    val fields = line.split(",")
    val stationId = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType,temperature)

  }

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(master = "local[*]", appName = "MaxTemperature")
    val lines=sc.textFile("data/1800.csv")
    val parsedLines= lines.map(parseLine)
    parsedLines.take(5).foreach(println)
    val maxTemperature= parsedLines.filter(x=> x._2=="TMAX")
    maxTemperature.take(5).foreach(println)
    val stationTemp= maxTemperature.map(x=> (x._1, x._3))
    val maxStationTemp = stationTemp.reduceByKey((x,y)=> max(x,y))
    val results= maxStationTemp.collect()
    maxStationTemp.foreach(println)
    results.foreach(println)




  }

}
