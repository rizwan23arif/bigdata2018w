package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "trained model", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplySpamClassifierConf(argv)
    
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
	
	val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model = args.model() + "/part-00000"
    
    val w = sc.textFile(model)
      .map(line => {
        val tokens = line.split(",")
        val feature = tokens(0).substring(1).toInt
        val weight = tokens(1).substring(0, tokens(1).length-1).toDouble
        (feature, weight)
      })
      .collect().toMap

    val weights = sc.broadcast(w)
    val inputFile = sc.textFile(args.input())

    val results = inputFile.map(line => {
      val tokens = line.split(" ")
      val docid = tokens(0)
      val trueLabel = tokens(1).trim()
      val features = tokens.slice(2, tokens.length).map(numStr => numStr.toInt)

      def spamminess(features: Array[Int]) : Double = {
        var score = 0d
        features.foreach(f => {
          val w = weights.value
          if (w.contains(f)) score += w(f)
        })
        score
      }

      val spam_Score = spamminess(features)
      val predictedLabel = if (spam_Score > 0) "spam" else "ham"

      (docid, trueLabel, spam_Score, predictedLabel)
    })

    results.saveAsTextFile(args.output())
  }
}
