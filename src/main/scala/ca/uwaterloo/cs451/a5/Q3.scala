package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "query date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q3 extends {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf3(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("Parquet: " + args.parquet())

    val shipdate = args.date()

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)

      if (args.text()) {
        
        val partRDD = sc.textFile(args.input() + "/part.tbl")
        val supplierRDD = sc.textFile(args.input() + "/supplier.tbl")
        val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
        
        val part = partRDD.map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
        val partHashmapRDD = sc.broadcast(part)
        
        val supplier = supplierRDD.map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(1))
        })
        .collectAsMap()
        val supplierHashmapRDD = sc.broadcast(supplier)
        
        val lineitem = lineitemRDD  
          .filter(line => {
            val tokens = line.split("\\|")
            tokens(10).contains(shipdate)
          })
          .flatMap(line => { 
            val tokens = line.split("\\|")
            if(partHashmapRDD.value.get(tokens(1)) != null && supplierHashmapRDD.value.get(tokens(2)) != null){
              List((Integer.parseInt(tokens(0)), partHashmapRDD.value.get(tokens(1)), supplierHashmapRDD.value.get(tokens(2))))
            }
            else List()
        })       
        .sortBy(_._1)
        var result = lineitem.take(20)
        for(data <- result) {
          println("(" + data._1 + "," + data._2.mkString(" ") + "," + data._3.mkString(" ") + ")")
        }
      }
    
      else {
        
        val sparkSession = SparkSession.builder.getOrCreate
        val partDF = sparkSession.read.parquet(args.input() + "/part")
        val partRDD = partDF.rdd
        val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
        val supplierRDD = supplierDF.rdd
        val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
        val lineitemRDD = lineitemDF.rdd
        
        val part = partRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
        val partHashmapRDD = sc.broadcast(part)
        
        val supplier = supplierRDD
        .map(line => (line(0), line(1)))
        .collectAsMap()
        val supplierHashmapRDD = sc.broadcast(supplier)
        
        val lineitem = lineitemRDD
          .filter(line => line(10).toString.contains(shipdate))
          .flatMap(line => {
              if(partHashmapRDD.value.get(line(1)) != null && supplierHashmapRDD.value.get(line(2)) != null){
                List((line(0).toString.toInt, partHashmapRDD.value.get(line(1)), supplierHashmapRDD.value.get(line(2))))
              }
              else List()
        })       
        .sortBy(_._1)
        var result = lineitem.take(20)
        for(data <- result) {
          println("(" + data._1 + "," + data._2.mkString(" ") + "," + data._3.mkString(" ") + ")")
        }
        
      }
  }
}
