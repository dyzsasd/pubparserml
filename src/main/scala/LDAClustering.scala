package com.multiposting.pubparserml

import com.multiposting.pubparserml.LDA._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by shuai on 12/2/14.
 */
object LDAClustering {
  def main(args: Array[String])={

    val numTopics = args(2).toInt
    val totalIter = args(3).toInt
    val burnInIter = args(4).toInt
    val alpha = 0.01
    val beta = 0.01

    val test = 0

    val dataPath = args(0)

    val conf = new SparkConf()
    val sc = new SparkContext(conf)


    val dict = sc.textFile(dataPath+"/diffs.txt").map(x=>{
      val items = x.split("[ ]+")
      (items(0),items(1))
    }).collect().toMap
    val broadcastedDict = sc.broadcast(dict);

    val stopwords = sc.textFile(dataPath+"/stopwords.txt").collect()
    val broadcastedStopwords = sc.broadcast(stopwords)

    val textRDD = sc.textFile(dataPath+"/Text.txt").map(x=>{
      val items = x.split("\t")
      (items(2),items(1).split(" ").filter(_.size>1))
    })



    val textRdd = sc.textFile(args(0)).map(_.split("\t"))

    val corpusRdd = textRdd.map(x=>Document(x(0).toInt,x(1).split(";").map(_.toInt)))

    val res = LDA.train(corpusRdd, numTopics, totalIter, burnInIter, alpha, beta)




  }
}
