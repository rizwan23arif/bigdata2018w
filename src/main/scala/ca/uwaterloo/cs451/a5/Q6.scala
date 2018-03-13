package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf6(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q6 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf6(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val date = args.date()

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)

      if (args.text()) {
        val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        
        val lineitem = lineitemRDD        
          .filter(line => {
            val tokens = line.split("\\|")
            tokens(10).contains(date)
          })
          .map(line => {
            val tokens = line.split("\\|")
            val l_returnflag = tokens(8).toString()
            val l_linestatus = tokens(9).toString()
            val l_quantity = tokens(4).toString().toDouble
            val l_extendedprice = tokens(5).toString().toDouble
            val l_discount = tokens(6).toString().toDouble
            val l_tax = tokens(7).toString().toDouble
            
            val disc_price = l_extendedprice * (1 - l_discount)
            val charge = l_extendedprice * (1 - l_discount) * (1 + l_tax)
            
            val count = 1
            
            ((l_returnflag, l_linestatus), (l_quantity, l_extendedprice, disc_price, charge, l_discount, count))            
          })
          
          .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
          .sortByKey()
          .map(pair => {
            val l_returnflag = pair._1._1
            val l_linestatus = pair._1._2
            val sum_qty = pair._2._1
            val sum_base_price = pair._2._2
            val sum_disc_price = pair._2._3
            val sum_charge = pair._2._4
            val avg_qty = pair._2._1 / pair._2._6
            val avg_price = pair._2._2 / pair._2._6
            val avg_disc = pair._2._5 / pair._2._6
            val count_order = pair._2._6
            (l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
          })           
          .collect()
          .foreach(println)
      }
    
      else{       
        val sparkSession = SparkSession.builder.getOrCreate
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        
        val lineitem = lineitemRDD        
          .filter(line => line(10).toString.contains(date))
          .map(line => {
            val l_returnflag = line(8).toString()
            val l_linestatus = line(9).toString()
            val l_quantity = line(4).toString().toDouble
            val l_extendedprice = line(5).toString().toDouble
            val l_discount = line(6).toString().toDouble
            val l_tax = line(7).toString().toDouble
            
            val disc_price = l_extendedprice * (1 - l_discount)
            val charge = l_extendedprice * (1 - l_discount) * (1 + l_tax)
            
            val count = 1
            
            ((l_returnflag, l_linestatus), (l_quantity, l_extendedprice, disc_price, charge, l_discount, count))            
          })
          
          .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6))
          .sortByKey()
          .map(pair => {
            val l_returnflag = pair._1._1
            val l_linestatus = pair._1._2
            val sum_qty = pair._2._1
            val sum_base_price = pair._2._2
            val sum_disc_price = pair._2._3
            val sum_charge = pair._2._4
            val avg_qty = pair._2._1 / pair._2._6
            val avg_price = pair._2._2 / pair._2._6
            val avg_disc = pair._2._5 / pair._2._6
            val count_order = pair._2._6
            (l_returnflag, l_linestatus, sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count_order)
          })           
          .collect()
          .foreach(println)
      }
  }
}
