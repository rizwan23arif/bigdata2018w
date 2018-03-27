package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ApplyEnsembleSpamConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "trained model", required = true)
  val method = opt[String](descr = "ensemble method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplyEnsembleSpamConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())
	
	val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)
        
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
	
	val method = args.method().trim()
	
    val modelGroup_x = args.model() + "/part-00000"
    val modelGroup_y = args.model() + "/part-00001"
    val modelBritney = args.model() + "/part-00002"

    def weights(modelDir: String) : Map[Int, Double] = {
      val modelFile = sc.textFile(modelDir)

      modelFile.map(line => {
          val tokens = line.split(",")
          val feature = tokens(0).substring(1).toInt
          val weight = tokens(1).substring(0, tokens(1).length-1).toDouble
          (feature, weight)
        })
        .collect().toMap
    }

    val weightsX = sc.broadcast( weights(modelGroup_x) )
    val weightsY = sc.broadcast( weights(modelGroup_y) )
    val weightsBrit = sc.broadcast( weights(modelBritney) )

    val textFile = sc.textFile(args.input())

    val results = textFile.map(line => {
      val tokens = line.split(" ")
      val docid = tokens(0)
      val trueLabel = tokens(1).trim()
      val features = tokens.slice(2, tokens.length).map(numStr => numStr.toInt)

      def spamminess(features: Array[Int], w: Map[Int, Double]) : Double = {
        var score = 0d
        features.foreach(f => if (w.contains(f)) score += w(f))
        score
      }

      def isSpam(score: Double) = if (score > 0) true else false

      var spam_Score = 0.0
      var predictedLabel = "unknown"

      val spam_ScoreX = spamminess(features, weightsX.value)
      val spam_ScoreY = spamminess(features, weightsY.value)
      val spam_ScoreBrit = spamminess(features, weightsBrit.value)

      if (method.equals("vote")) {
        var spamVotes = 0
        var hamVotes = 0
        if (isSpam(spam_ScoreX))    spamVotes += 1 else hamVotes += 1
        if (isSpam(spam_ScoreY))    spamVotes += 1 else hamVotes += 1
        if (isSpam(spam_ScoreBrit)) spamVotes += 1 else hamVotes += 1
        spam_Score = spamVotes - hamVotes
        predictedLabel = if (spam_Score > 0) "spam" else "ham"
      }
      else {
        spam_Score = (spam_ScoreX + spam_ScoreY + spam_ScoreBrit) / 3.0
        predictedLabel = if (spam_Score > 0) "spam" else "ham"
      }

      (docid, trueLabel, spam_Score, predictedLabel)
    })

    results.saveAsTextFile(args.output())
  }
}
