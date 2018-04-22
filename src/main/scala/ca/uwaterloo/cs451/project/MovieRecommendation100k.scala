/* Made on April 20, 2018
Author: Rizwan Aarif
This program recommends the top 10 movies to the user. */

package ca.uwaterloo.cs451.project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.math.sqrt

object MovieRecommendation100k {
	val log = Logger.getLogger(getClass().getName())
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("MovieRecommendation100k")
    val sc = new SparkContext(conf)
    
    val textfile = sc.textFile("ml-100k/u.data")

    // Mapping ratings (user ID => movie ID, rating)
	// Emit every movie rated together by the same user.
	
    val ratings = textfile
		.map(l => l.split("\t"))
		.map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble)))
		
	//Self-join to find every combination.
	val joinRatings = ratings.join(ratings) 
		.filter(filterDuplicates)

    // At this point our RDD consists of userID => ((movieID, rating), (movieID, rating))	    	
    val moviePairs = joinRatings.map(makePairs)
	
	// At this point our RDD consists of (movie1, movie2) => (rating1, rating2)
    // Group ratings to perform similarity		
    val moviePairRatings = moviePairs.groupByKey()
	
	// At this point our RDD consists of (movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
    
    if (args.length > 0) {
      val scoreThreshold = 0.95
      val coOccurenceThreshold = 50.0     
      val movieID:Int = args(0).toInt
      
	  // Filtering the movies with scoreThreshold and coOccurenceThreshold
      val filteredResults = moviePairSimilarities.filter( x =>
        {
          val pair = x._1
          val sim = x._2
          (pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
        }
      )
        
      // Sort by quality score.
      val results = filteredResults.map( x => (x._2, x._1)).sortByKey(false).take(10)
 
	  val moviesWithNames = MovieNames()	
	  
	  //Printing the recommendations
      println("\nTop 10 recommended movies for " + moviesWithNames(movieID))
      for (result <- results) {
        val sim = result._1
        val pair = result._2
        var similarMovieID = pair._1
        if (similarMovieID == movieID) {
          similarMovieID = pair._2
        }
        println(moviesWithNames(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
      }
    }
  }
  
  /** This function generates a map of Movie IDs to Movie Names */
  def MovieNames() : Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Map of Strings to Integers.
    var MoviesCollection:Map[Int, String] = Map()
    
     val lines = Source.fromFile("ml-100k/u.item").getLines()
     for (line <- lines) {
       var tokens = line.split('|')
       if (tokens.length > 1) {
        MoviesCollection += (tokens(0).toInt -> tokens(1))
       }
     }    
     return MoviesCollection
  }
  
  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))
  def makePairs(userRatings:UserRatingPair) = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val rating1 = movieRating1._2
    val movie2 = movieRating2._1
    val rating2 = movieRating2._2
    
    ((movie1, movie2), (rating1, rating2))
  }
  
  def filterDuplicates(userRatings:UserRatingPair):Boolean = {
    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2
    
    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    
    return movie1 < movie2
  }
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
}
