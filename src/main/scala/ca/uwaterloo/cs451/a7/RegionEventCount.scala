/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class RegionEventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object RegionEventCount {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new RegionEventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("RegionEventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)
    
    val goldx_min = -74.0144185
    val goldx_max = -74.013777
    
    val goldy_min = 40.7138745
    val goldy_max = 40.7152275
    
    val citix_min = -74.012083
    val citix_max = -74.009867
    
    val citiy_min = 40.720053
    val citiy_max = 40.7217236

    val wc = stream.map(_.split(","))      
      
      .map(tuple => {
        val colour = tuple(0)
        val dropcor = if (colour == "green") 
          {List(tuple(8).toDouble, tuple(9).toDouble)}
        else 
          {List(tuple(10).toDouble, tuple(11).toDouble)}
        (dropcor, 1)
      })
      .filter(p => ((p._1(0) > goldx_min ) && (p._1(0) < goldx_max) && (p._1(1) > goldy_min) && (p._1(1) < goldy_max) ) || 
                   ((p._1(0) > citix_min) && (p._1(0) < citix_max) && (p._1(1) > citiy_min) && (p._1(1) < citiy_max)) ) 
      .map(p => {
        val droploc = if ((p._1(0) > goldx_min ) && (p._1(0) < goldx_max) && (p._1(1) > goldy_min) && (p._1(1) < goldy_max) )
          "goldman"
         else 
           "citigroup"       
        (droploc, 1)
      })

      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
