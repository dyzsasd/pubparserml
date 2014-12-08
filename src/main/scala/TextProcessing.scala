import java.io.{FileWriter, BufferedWriter}
import java.nio.charset.Charset
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.mutable.ArrayBuffer
import scala.math.random
import org.apache.spark._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating



object Processing {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val path = "hdfs:///user/szhang/pubparser/resources/new"

    //loading and parsing data
    val rawData = sc.textFile(path,args(0).toInt)
    //val lines = rawData.flatMap(x=>x._2.split("\n"))
    val labeledLines = rawData.filter(_.split("\t").length==2).map(x=>{
      val items = x.split("\t")
      (items(1), items(0))
    })
    val labeledWordVectors = preprocess(labeledLines)
    System.out.println("labeledWordVectors' partition number is "+labeledWordVectors.partitions.length.toString)
    //labeledWordVectors.persist(StorageLevel.MEMORY_ONLY_SER)

    //create word2int dictionary
    val wordPairSet = labeledWordVectors.flatMap(x=>x._2.distinct.map((_,1)))
    val wordCount = wordPairSet.reduceByKey((a,b)=>a+b)
    val filteredWordcount = wordCount.filter(x=>x._2>1000 && x._2<1000000)
    val wordSet = filteredWordcount.collect.toSeq.sortBy(_._2)

    val indexedWords = wordSet.map(_._1).zipWithIndex
    val wordDict=collection.mutable.Map[String,Int]()

    for(i <- 0 until indexedWords.length){
      wordDict+=indexedWords(i)
    }

    //convert word vector to digital vector
    val broadcastedDict = sc.broadcast(wordDict)
    val labeledVecotrs = labeledWordVectors.map(x=>{
      val indexNbTuple = x._2.filter(broadcastedDict.value.contains(_))
        .map(y=>(broadcastedDict.value(y),1))
        .groupBy(_._1)
        .toArray
        .sortBy(_._1)
        .map(y=>(y._1,y._2.length))

      (x._1,indexNbTuple)
    })
    //val sample = labeledVecotrs.sample(false,0.2)
    val indexedLabeledVectors = labeledVecotrs.zipWithIndex()
    //indexedLabeledVectors.persist(StorageLevel.MEMORY_ONLY_SER)

    val coordinatorMatrix = indexedLabeledVectors.flatMap(x=>{
      val rowId = x._2
      val columns = x._1._2
      columns.map(y=>((rowId,y._1),y._2))
    })
    val labels = indexedLabeledVectors.map(x=>(x._2, x._1._1))

    val ratings = coordinatorMatrix.map(x=>Rating(x._1._1.toInt,x._1._2,x._2))
    ratings.persist(StorageLevel.MEMORY_ONLY_SER)

    // Build the recommendation model using ALS
    val rank = args(1).toInt
    val numIterations = args(2).toInt
    val model = ALS.train(ratings, rank, numIterations, 0.01)

    model.userFeatures.map(userLine=>{
      val id = userLine._1
      val vectors = userLine._2

      val res = id+";"+vectors.mkString(";")
      res
    }).saveAsTextFile("hdfs:///user/szhang/pubparser/resources/articleFeatures")
    model.productFeatures.map(productLine=>{
      val id = productLine._1
      val vectors = productLine._2

      val res = id+";"+vectors.mkString(";")
      res
    }).saveAsTextFile("hdfs:///user/szhang/pubparser/resources/wordFeatures")

    coordinatorMatrix.saveAsTextFile("hdfs:///user/szhang/pubparser/resources/coordinatorMatrix")
    labels.saveAsTextFile("hdfs:///user/szhang/pubparser/resources/labels")


    val bw = new BufferedWriter(new FileWriter("wordset.txt"))
    wordSet.foreach(x=>{
      bw.write(x._1+"\t")
      bw.write(x._2.toString  )
      bw.newLine()
    })
    bw.close()

  }



  def preprocess(lines:RDD[(String,String)]):RDD[(String, Array[String])]={
    val minStringLen = 3
    val maxStringLen = 10

    val word2Array = (str:String) =>{
      val len = str.length

      val res = new ArrayBuffer[String]()

      if(len<minStringLen)
        new Array[String](0)
      else{
        for(winlen<-minStringLen to Math.min(len,maxStringLen)){
          for(startindex<-0 to len-winlen){
            res+=str.substring(startindex,startindex+winlen)
          }
        }
      }
      res.toArray
    }

    val labeledWordVectors = lines.map(x=>(x._1,x._2.split(" ").filter(_!=" ")))
    val parsedWordVectors = labeledWordVectors.map(x=>(x._1,x._2.flatMap(word2Array(_))))

    parsedWordVectors
  }

}
