/* Author: Rizwan Aarif
   Date: 30.01.2018 */
   
package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf2(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("ComputeBigramRelativeFrequencyStripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList
		else List()
      })
      .map(ComputeBigramRelativeFrequencyPairs => (ComputeBigramRelativeFrequencyPairs, 1))
      .reduceByKey(_ + _)
	  
    val wc = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => p.mkString(" ")).toList
		else List()
      })
      .map(pairs => (pairs, 1))
	  
    val wordCount = wc.map(wc => (wc._1.split(" ")(0), 1))
      .reduceByKey(_ + _)
	  
    val count = counts.map(counts => (counts._1.split(" ")(0), counts._1.split(" ")(1) + " " + counts._2))
    val items = count.join(wordCount)

    val inter = items.map(items => (items._1, items._2._1.split(" ")(0) + "=" + (items._2._1.split(" ")(1).toDouble / (items._2._2))))
      .groupByKey()
      .map(n => (n._1, n._2.toList.mkString((","))))
	  
    val result = inter.map(r => ("(" + r._1 + ",\t" + "{" + r._2 + "})"))

    result.saveAsTextFile(args.output())
  }
}
