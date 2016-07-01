package univ.bigdata.course.part1.preprocessing

import java.io.IOException

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.{Movie, MovieReview}
import univ.bigdata.course.part1.preprocessing.internal.MovieIOInternals
import univ.bigdata.course.part2.Recommendation
import univ.bigdata.course.part2.Recommendation.toID

object MovieIO {
  // Reads file and creates new movies from it
  def readMovies(inputFile: String): RDD[Movie] = {
  // Read reviews from file
    val reviews = getMovieReviews(inputFile)
  // Creates Movies from reviews
    batchMovieReviews(reviews)
  }
  // Creates reviews from file 
  @throws[IOException]
  def getMovieReviews(inputFilePath: String): RDD[MovieReview] = {
    val fsPath = if(inputFilePath.contains("://")) inputFilePath.split("://")(1) else inputFilePath
    val hPath = if(inputFilePath.contains("://")) inputFilePath else "file://" + inputFilePath

    val path = if(fileExists(fsPath)) fsPath else hPath

    val reviewsLines = SparkMain.sc.textFile(path)
    reviewsLines.map(MovieIOInternals.lineToReview)
  }

  def fileExists(path: String): Boolean = {
    try{
      SparkMain.sc.textFile(path).first()
      true
    } catch {
      case e:Exception => false
    }
  }

  // Creates Movies from reviews
  def batchMovieReviews(reviews: RDD[MovieReview]): RDD[Movie] = {
  // Group reviews by movies ID and create new movie by passing it's ID and reviews vector
    val reviewsGrouped: RDD[(String, Iterable[MovieReview])] = reviews.groupBy(_.movieId)
    reviewsGrouped.map {
      case (id, movieReviews) => new Movie(id, movieReviews.toVector)
    }
  }
}
