import org.apache.spark._

object FriendsByAge {

  def parseLine(line:String)={
    val fields=line.split(",")
    val age=fields(2).toInt
    val numFriends=fields(3).toInt
    (age,numFriends)
  }
  def main(args:Array[String])={

    val sc= new SparkContext(master = "local[*]", appName = "FriendsByAge")
    val data=sc.textFile("data/fakefriends-noheader.csv")
    val rdd= data.map(parseLine)
    val totalsByAge=rdd.mapValues(x=>(x,1)).reduceByKey((x,y)=>(x._1 + y._1, x._2+ y._2))
    /** anlamak için böldüm
     ------x._1 +y._1 kaç tane arkadaş var,
    -------y._1,  x._2+ y._2  ne kadar insan var
    val totalsByAge2 = rdd.mapValues(x=>(x,1))
    println("totalbyAge yazdırılıyor")
    totalsByAge2.foreach(println)
    val reduceBykeys= totalsByAge2.reduceByKey((x,y)=>(x._1 + y._1, x._2+ y._2))
    println("reducebyKey yazdırılıyor")
    reduceBykeys.foreach(println)
     */
    val avarageByAge= totalsByAge.mapValues(x=> x._1/x._2)
    val results = avarageByAge.collect()
    results.sorted.foreach(println)






  }
}
