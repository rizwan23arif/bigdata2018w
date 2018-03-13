package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf4(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q4 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf4(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val shipdate = args.date()

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)

      if (args.text()) {
        
        val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
        val customerRDD = sc.textFile(args.input() + "/customer.tbl")
        val nationRDD = sc.textFile(args.input() + "/nation.tbl")
        
        val customer = customerRDD.map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(3))
        })
        .collectAsMap()
        val customerHashmapRDD = sc.broadcast(customer)
        
        val nation = nationRDD.map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
        val nationHashmapRDD = sc.broadcast(nation)
        
        val lineitem = lineitemRDD  
          .filter(line => {
            val tokens = line.split("\\|")
            tokens(10).contains(shipdate)
          })
          .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(10))
        })
        
        val orders = ordersRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        
        orders.cogroup(lineitem)
        .filter(pair => {
          var l_shipdate = pair._2._2
          shipdate.iterator.hasNext
        })
        .map(pair => {
          var count = 0
          var shipdateIterator = pair._2._2.iterator
          while (shipdateIterator.hasNext) {
            shipdateIterator.next()
            count += 1
          }
          (pair._1, pair._2._1.iterator.next(), count)
        })
        .map(pair => (customerHashmapRDD.value(pair._2), pair._3))
        .reduceByKey(_+_)
        .map(pair => (pair._1.toInt, (nationHashmapRDD.value(pair._1), pair._2)))
        .sortByKey()
        .collect()
        .foreach(pair => println("(" + pair._1 + "," + pair._2._1 + "," + pair._2._2 + ")"))        
        
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
        
        val customer = customerRDD
        .map(line => (line(0), line(3)))
        .collectAsMap()
        val customerHashmapRDD = sc.broadcast(customer)
        
        val nation = nationRDD
        .map(line => (line(0), line(1))
        )
        .collectAsMap()
        val nationHashmapRDD = sc.broadcast(nation)
        
        val lineitem = lineitemRDD  
          .filter(line => line(10).toString.contains(shipdate))
          .map(line => (line(0), line(10)))
        
        val orders = ordersRDD
        .map(line => (line(0), line(1)))
        
        orders.cogroup(lineitem)
        .filter(pair => {
          var l_shipdate = pair._2._2
          shipdate.iterator.hasNext
        })
        .map(pair => {
          var count = 0
          var shipdateIterator = pair._2._2.iterator
          while (shipdateIterator.hasNext) {
            shipdateIterator.next()
            count += 1
          }
          (pair._1, pair._2._1.iterator.next(), count)
        })
        .map(pair => (customerHashmapRDD.value(pair._2), pair._3))
        .reduceByKey(_+_)
        .map(pair => (pair._1.toString.toInt, (nationHashmapRDD.value(pair._1), pair._2)))
        .sortByKey()
        .collect()
        .foreach(pair => println("(" + pair._1 + "," + pair._2._1 + "," + pair._2._2 + ")"))          
      }
  }
}
