package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf2(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q2 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf2(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val date = args.date()

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)

      if (args.text()) {
        val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        
        val lineitem = lineitemRDD        
          .filter(line => {
            val tokens = line.split("\\|")
            tokens(10).toString.startsWith(date)
          })
          .map(line => {
            val tokens = line.split("\\|")
            (tokens(0), tokens(10))
          })
        
        val orders = ordersRDD
          .map(line => {
            val tokens = line.split("\\|")
            (tokens(0), tokens(6))
            })
         
        var resultRDD = orders.cogroup(lineitem)
          .filter(pair => pair._2._2.iterator.hasNext)
          .map(pair => (pair._1.toString.toLong, pair._2._1.iterator.next()))
          .sortByKey()
          
        resultRDD.take(20)
        .foreach(pair => println("(" + pair._2 + "," + pair._1 + ")"))
      }
    
      else{       
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
        val ordersRDD = ordersDF.rdd
        
        val lineitem = lineitemRDD
        .filter(line => line(10).toString.startsWith(date))
        .map(line => (line(0), line(10)))

        val orders = ordersRDD
        .map(line => (line(0), line(6)))

        var resultRDD = orders.cogroup(lineitem)
          .filter(pair => pair._2._2.iterator.hasNext)
          .map(pair => (pair._1.toString.toLong, pair._2._1.iterator.next()))
          .sortByKey()
          
        resultRDD.take(20)
        .foreach(pair => println("(" + pair._2 + "," + pair._1 + ")"))
      }
  }
}
