/* Author: Rizwan Aarif
   Date: 30.01.2018 */
   
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf3(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PairsPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    var N = textFile.count()
    val threshold = args.threshold()


    val uniqueWords = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 0) tokens.take(Math.min(tokens.length, 40)).distinct 
		    else List()
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    val hash_map = sc.broadcast(uniqueWords)

    val pairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val words = tokens.take(Math.min(tokens.length, 40)).distinct
        if (tokens.length > 1) words.flatMap(a => words.map(b => (a, b))).filter(a => a._1 != a._2)
        else List()
      })
	  
    val PMIpair = pairs.map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .filter(a => (a._2 >= threshold))

    val split = PMIpair.map(PMIpair => (PMIpair._1._1, PMIpair._1._2, PMIpair._2))
    val m = split.map(split => (split._1 + "," + split._2, hash_map.value(split._1), hash_map.value(split._2), split._3))
    val result = m.map(m => (("(" + m._1 + ")\t" + "(" + scala.math.log10((m._4 * N).toDouble / (m._2).toDouble / (m._3).toDouble)), m._4 + ")"))

    result.saveAsTextFile(args.output())
  }
}
