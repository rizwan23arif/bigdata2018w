package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q7 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf7(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val date = args.date()

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)

      if (args.text()) {
        val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        val customerRDD = sc.textFile(args.input() + "/customer.tbl")
        val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        
        val customer = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
        val customerHashmapRDD = sc.broadcast(customer)
  
        val lineitem = lineitemRDD
          .filter(line => {
            val tokens = line.split("\\|")
            tokens(10) > date
          })
          .map(line => {
            val tokens = line.split("\\|")
            (tokens(0), tokens(5).toDouble * (1 - tokens(6).toDouble))
          })
          .reduceByKey(_+_)
  
        val orders = ordersRDD
          .filter(line => {
            val tokens = line.split("\\|")
            tokens(4) < date
          })
          .map(line => {
            val tokens = line.split("\\|")
            val name = customerHashmapRDD.value(tokens(1))
            val o_orderdate = tokens(4)
            val o_shippriority = tokens(7)
            (tokens(0), (name, o_orderdate, o_shippriority))
          })
  
        lineitem.cogroup(orders)
          .filter(pair => pair._2._1.iterator.hasNext && pair._2._2.iterator.hasNext)
          .map(pair => {
            val l_orderkey = pair._1
            val revenue = pair._2._1.iterator.next()
  
            val ordersIterator = pair._2._2.iterator.next()
            val name = ordersIterator._1
            val o_orderdate = ordersIterator._2
            val o_shippriority = ordersIterator._3
            (revenue, (l_orderkey, name, o_orderdate, o_shippriority))
          })
          .sortByKey(false)
          .take(10)
          .map(pair => (pair._2._2, pair._2._1, pair._1, pair._2._3, pair._2._4))
          .foreach(println)
      }
    
      else {   
        
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        val customerDF = sparkSession.read.parquet(args.input() + "/customer")
        val customerRDD = customerDF.rdd
        val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
        val ordersRDD = ordersDF.rdd
        
        val customer = customerRDD
        .map(line => (line(0), line(1))).collectAsMap()
        val customerHashmapRDD = sc.broadcast(customer)
  
        val lineitem = lineitemRDD
          .filter(line => line(10).toString > date)
          .map(line => (line(0), line(5).toString.toDouble * (1 - line(6).toString.toDouble)))
          .reduceByKey(_+_)
  
        val orders = ordersRDD
          .filter(line => line(4).toString < date)
          .map(line => {
            val name = customerHashmapRDD.value(line(1))
            val o_orderdate = line(4)
            val o_shippriority = line(7)
            (line(0), (name, o_orderdate, o_shippriority))
          })
  
        lineitem.cogroup(orders)
          .filter(pair => pair._2._1.iterator.hasNext && pair._2._2.iterator.hasNext)
          .map(pair => {
            val l_orderkey = pair._1
            val revenue = pair._2._1.iterator.next()
  
            val ordersIterator = pair._2._2.iterator.next()
            val name = ordersIterator._1
            val o_orderdate = ordersIterator._2
            val o_shippriority = ordersIterator._3
            (revenue, (l_orderkey, name, o_orderdate, o_shippriority))
          })
          .sortByKey(false)
          .take(10)
          .map(pair => (pair._2._2, pair._2._1, pair._1, pair._2._3, pair._2._4))
          .foreach(println)
      }
  }
}
