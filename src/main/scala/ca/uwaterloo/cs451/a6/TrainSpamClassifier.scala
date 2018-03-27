package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._

class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output trained model", required = true)
  val shuffle = opt[Boolean]()
  verify()
}

object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new TrainSpamClassifierConf(argv)
    
    log.info("Input: " + args.input())
    log.info("Model: " + args.model())
    log.info("Shuffle: " + args.shuffle())
	
	val conf = new SparkConf().setAppName("TrainSpamClassifier")
    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    val shuffle = args.shuffle()
    val textFile = sc.textFile(args.input())

    var trainSet = textFile.map(line => {
      val tokens = line.split(" ")
      val docid = tokens(0)
      val isSpam = if (tokens(1).trim().equals("spam")) 1 else 0
      val features = tokens.slice(2, tokens.length).map(numStr => numStr.toInt)
      val rand = scala.util.Random.nextInt
      (0, (docid, isSpam, features, rand))
    })

    if (shuffle) {
      trainSet = trainSet.sortBy(pair => pair._2._4)
    }

    val trained = trainSet.groupByKey(1)
      .flatMap(pair => {
        val instances = pair._2
        val w = Map[Int, Double]()
        val delta = 0.002

        def spamminess(features: Array[Int]): Double = {
          var score = 0d
          features.foreach(f => if (w.contains(f)) score += w(f))
          score
        }

        instances.foreach(instance => {
          val docid = instance._1
          val isSpam = instance._2
          val features = instance._3
          val score = spamminess(features)
          val prob = 1.0 / (1 + Math.exp(-score))
          features.foreach(f => {
            val newWeight = w.getOrElse(f, 0.0) + ((isSpam - prob) * delta)
            w(f) = newWeight
          })
        })
        w
      })      
    trained.saveAsTextFile(args.model())
  }
}