/* Author: Rizwan Aarif
   Date: 30.01.2018 */
   
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf4(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("StripesPMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    var N = textFile.count()
    val threshold = args.threshold()
	
    val uniqueWords = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.distinct.map(word => (word, 1)) else List().map(word => (word, 1))
      })
      .reduceByKey(_ + _).collectAsMap()

    val hash_map = sc.broadcast(uniqueWords)

    val pairs = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.distinct.flatMap(a => tokens.map(b => (a, b))).distinct.filter(a => a._1 != a._2)
        else List().flatMap(a => tokens.map(b => (a, b))).distinct.filter(a => a._1 != a._2)
      })
	  
    val PMIpair = pairs.map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .filter(a => (a._2 >= threshold))

    val split = PMIpair.map(PMIpair => (PMIpair._1._1, PMIpair._1._2, PMIpair._2))
    val PMI = split.map(split => (split._1, split._2 + "=(" + scala.math.log10((split._3 * N).toDouble / hash_map.value(split._1).toDouble / hash_map.value(split._2)) + "," + split._3 + ")"))
      .groupByKey()
      .map(n => (n._1, n._2.toList.mkString((","))))
	  
    val result = PMI.map(PMI => ("(" + PMI._1 + ",\t" + "{" + PMI._2 + "})"))

    result.saveAsTextFile(args.output())

  }
}

