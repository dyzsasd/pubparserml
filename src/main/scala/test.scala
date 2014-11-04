import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.math.random
import org.apache.spark._


object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "YOUR_SPARK_HOME/README.md"

    System.setProperty("spark.executor.instances", "3")
    val conf = new SparkConf().setAppName("Spark-Test")
                              //.setSparkHome(System.getenv("SPARK_HOME"))

    conf.getAll.foreach(println)
    val sc = new SparkContext("local","test")
    //val sc = new SparkContext(conf)

    val a = Array[Int](4,2,3,4,5,6,7,8,9,0,1,2,3,4,2,3,6,4,7,9,0,7,6,5,4,34,4,5,6,7,65,3,43,47,678,3,6,3)

    val rdd = sc.parallelize(a,3)

    val newrdd = rdd.map(x=>(x%3,x))
    val group = newrdd.partitionBy(new HashPartitioner(3))
    val transrdd = group.mapPartitionsWithIndex((partitionID,items)=>{
      val newitems = items.map(x=>(partitionID,x))
      newitems
    },preservesPartitioning = true)
    val res = transrdd.collect()
      res.foreach(println)
  }
}

object SparkPi {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://cluster3:7077")
    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}