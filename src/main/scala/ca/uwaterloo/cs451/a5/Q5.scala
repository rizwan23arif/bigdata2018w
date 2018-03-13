package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q5 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf5(argv)

    log.info("Input: " + args.input())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

      if (args.text()) {
        
        val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        val customerRDD = sc.textFile(args.input() + "/customer.tbl")
        val nationRDD = sc.textFile(args.input() + "/nation.tbl")
        
        val canada = 3
        val us = 24
        
        val customer = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(3).toInt)
        })
        .collectAsMap()
        val customerHashmapRDD = sc.broadcast(customer)
        
        val nation = nationRDD.map(line => {
          val tokens = line.split("\\|")
          (tokens(0).toInt, tokens(1))
        })
        .collectAsMap()
        val nationHashmapRDD = sc.broadcast(nation)
        
        val orders = ordersRDD
        .filter(line => {
          val tokens = line.split("\\|")
          val c_nationkey = customerHashmapRDD.value(tokens(1))
          c_nationkey == 3 || c_nationkey == 24
        })
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), customerHashmapRDD.value(tokens(1)))
        })
        
        val lineitem = lineitemRDD  
        .map(line => {
          val tokens = line.split("\\|")
          val yearMonth = tokens(10).substring(0, 7)
          (tokens(0), yearMonth)
        })
        
        lineitem.cogroup(orders)
        .filter(pair => pair._2._2.iterator.hasNext)
        .flatMap(pair => {
          val nationKey = pair._2._2
          pair._2._1.map(date => ((nationKey, date), 1))
        })
        .reduceByKey(_+_)
        .sortByKey()
        .collect()
        .foreach(pair => println("(" + pair._1._1.mkString+ "," + pair._1._2 + "," + pair._2 + ")"))    
        
      }
    
      else {
        
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
        val ordersRDD = ordersDF.rdd
        val customerDF = sparkSession.read.parquet(args.input() + "/customer")
        val customerRDD = customerDF.rdd
        val nationDF = sparkSession.read.parquet(args.input() + "/nation")
        val nationRDD = nationDF.rdd
        
        val canada = 3
        val us = 24
        
        val customer = customerRDD
        .map(line => (line(0), line(3).toString.toInt))
        .collectAsMap()
        val customerHashmapRDD = sc.broadcast(customer)
        
        val nation = nationRDD
        .map(line => (line(0).toString.toInt, line(1)))
        .collectAsMap()
        val nationHashmapRDD = sc.broadcast(nation)
        
        val orders = ordersRDD
        .filter(line => {
          val c_nationkey = customerHashmapRDD.value(line(1))
          c_nationkey == 3 || c_nationkey == 24
        })
        .map(line => (line(0), customerHashmapRDD.value(line(1))))
        
        val lineitem = lineitemRDD
        .map(line => {
          val yearMonth = line(10).toString.substring(0, 7)
          (line(0), yearMonth)
        })

        lineitem.cogroup(orders)
        .filter(pair => pair._2._2.iterator.hasNext)
        .flatMap(pair => {
          val nationKey = pair._2._2
          pair._2._1.map(date => ((nationKey, date), 1))
        })
        .reduceByKey(_+_)
        .sortByKey()
        .collect()
        .foreach(pair => println("(" + pair._1._1.mkString+ "," + pair._1._2 + "," + pair._2 + ")"))         
      }
  }
}
