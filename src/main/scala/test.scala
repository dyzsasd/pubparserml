import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.math.random
import org.apache.spark._

object InverseOrder {
  def main(args: Array[String]) {
    val conf = new SparkConf()

    val sc = new SparkContext(conf)

    var path = ""
    if(args.length>0){
      path = args(0)
    }else{
      path = "hdfs:///user/szhang/pubparser/resources/outputtest.csv"
    }
    val rawData = sc.textFile(path)

    val indexedData = rawData.zipWithIndex()

    val endedData = indexedData.map(x=>{
      val text = x._1
      var restext:String = text
      if (text.split("\t").length==3){
        restext = text+"endline"
      }

      (x._2, restext)
    })

    val inversData = endedData.sortByKey(ascending = false,numPartitions = 20)

    inversData.map(x=>(x._1+"\t"+x._2)).saveAsTextFile("hdfs:///user/szhang/pubparser/resources/indexed")
  }
}