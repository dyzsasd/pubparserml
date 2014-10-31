import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._

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