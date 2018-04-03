package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.dstream.{DStream, MapWithStateDStream}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable
import scala.collection.mutable._

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

case class tupleClass (currentV: Int, timeSt: String, previousV: Int) extends Serializable 


object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())
  
  val GREEN = "green"
  val YELLOW = "yellow"
  
  val goldy_max = 40.7152275
  val goldy_min = 40.7138745
  
  val goldx_max = -74.013777
  val goldx_min = -74.0144185
  
  val citiy_max =  40.7217236
  val citiy_min =  40.720053
  
  val citix_max = -74.009867
  val citix_min = -74.012083
  
  val GS = "GS"
  val CG = "CG"
  
  val NONEV = "None"
  
  def company(longV:Float, latV:Float): String = {
    if(longV >= goldx_min && longV <= goldx_max && latV >= goldy_min && latV <= goldy_max) {return GS}
    else if(longV >= citix_min && longV <= citix_max && latV >= citiy_min && latV <= citiy_max) {return CG}
    else  {return NONEV}
  }
  
  def filleredTaxi(trip: Array[String]): List[Tuple2[String, Int]] = {
    var taxiType = trip(0)
    var longV = 0F
    var latV = 0F
    if(GREEN.equalsIgnoreCase(taxiType)){
      longV = trip(8).toFloat
      latV = trip(9).toFloat
    }
    else if(YELLOW.equalsIgnoreCase(taxiType)){
      longV = trip(10).toFloat
      latV = trip(11).toFloat
    }
    
    var companyN = company(longV, latV)
    
    if(companyN.equalsIgnoreCase(GS)){
      var retList = new ListBuffer[Tuple2[String,Int]]()
      var tuple:Tuple2[String,Int] = ("goldman",1)
      retList+=tuple
      return retList.toList
    }
    else if(companyN.equalsIgnoreCase(CG)){
      var retList = new ListBuffer[Tuple2[String,Int]]()
      var tuple:Tuple2[String,Int] = ("citigroup",1)
      retList+=tuple
      return retList.toList
    }
    else{
      return List()
    }
    
  }
  
  def trackStateFunc(batchTime: Time, key: String, value: Option[Int], state: State[tupleClass]): Option[(String,tupleClass)] = {
    
    var previousV = 0
    if(state.exists()){
      var currentState = state.get()
      previousV = currentState.currentV 
    }
    var currentV = value.getOrElse(0).toInt
    var batchTimeMs = batchTime.milliseconds
    if((currentV >= 10) && (currentV>= (2*previousV))){
       if(key.equalsIgnoreCase("goldman")){
          println(s"Number of arrivals to Goldman Sachs has doubled from $previousV to $currentV at $batchTimeMs")
       }
       else if(key.equalsIgnoreCase("citigroup")){
          println(s"Number of arrivals to Citigroup has doubled from $previousV to $currentV at $batchTimeMs")
        }       
    }
    var tClassObject = tupleClass(currentV = currentV, timeSt = "%08d".format(batchTimeMs), previousV = previousV)
    state.update(tClassObject)
    var output = (key,tClassObject)
    Some(output)
  }
  
  
  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)
    
    //val initialRDD = spark.sparkContext.parallelize(List(("goldman", 0), ("citigroup", 0)))
    val stateSpec = StateSpec.function(trackStateFunc _)//.initialState(initialRDD)
    
    val wc = stream.map(_.split(",")).flatMap(trip => filleredTaxi(trip))
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10)).mapWithState(stateSpec)//.persist()
      
    val snapShotsStreamRDD = wc.stateSnapshots()//.map{case(k,v) => (k,(v.currentV,v.timeSt,v.previousV))}
    var output = args.output()
        
     snapShotsStreamRDD.foreachRDD((rdd, time) =>{
       var updatedRDD = rdd.map{case(k,v) => (k,(v.currentV,v.timeSt,v.previousV))}
       updatedRDD.saveAsTextFile(output+ "/part-"+"%08d".format(time.milliseconds))
     }) 
   
    //snapShotsStreamRDD.saveAsTextFiles(args.output()+"/part") 
    
    snapShotsStreamRDD.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()
    
    //wc.foreachRDD(r => r.saveAsTextFile("path/part"))
    
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
