/* Made on April 20, 2018
Author: Rizwan Aarif
This program finds the total ratings of all the movies of 1M MovieLens Dataset. */

package ca.uwaterloo.cs451.project

import org.apache.spark._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec


class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object MostRatedlatest {  
  val log = Logger.getLogger(getClass().getName())
 
  def main(argv: Array[String]) {     
    val args = new Conf3(argv)
    
    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    
    val conf = new SparkConf().setAppName("MostRatedlatest")
    val sc = new SparkContext(conf)
    
    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
    
    // Broadcasting moviesID ==> moviesNames
    var broadcastedMovies = sc.broadcast(MovieNames(args.input()))
    
    // Reading from the ratings file
    val lines = sc.textFile(args.input() + "/ratings.csv")
    
	// Generating moviesID and Count
    val movies = lines
		.map(x => (x.split(",")(1).toInt, 1))
		.reduceByKey( (x, y) => x + y )
    
    // Sorting movies on the basis of count
    val sortedMovies = movies
		.map( x => (x._2, x._1) )
		.sortByKey()
    
    // Extracting movies names from movies ID.
    val moviesWithNames = sortedMovies
		.map( x  => (broadcastedMovies.value(x._2), x._1) )
		.saveAsTextFile(args.output())
  }
  
  /** This function generates a map of Movie IDs to Movie Names */
  def MovieNames(path: String) : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Map of Strings to Integers.
    var MoviesCollection:Map[Int, String] = Map()
    
     val lines = Source.fromFile(path + "/movies.csv").getLines()
     for (line <- lines) {
       var tokens = line.split(",")
       if (tokens.length > 1) {
        MoviesCollection += (tokens(0).toInt -> tokens(1))
       }
     }    
     return MoviesCollection
  }
}

