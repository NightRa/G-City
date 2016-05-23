package univ.bigdata.course.part1.preprocessing

import java.io.IOException

import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.{Movie, MovieReview}
import univ.bigdata.course.part1.preprocessing.internal.MovieIOInternals

object MovieIO {
  @throws[IOException]
  def getMovieReviews(inputFilePath: String): RDD[MovieReview] = {
    val reviewsLines = SparkMain.sc.textFile(inputFilePath)
    reviewsLines.map(MovieIOInternals.lineToReview)
  }

  /*@throws[IOException]
  def readMoviesFunctions(inputFilePath: String): MoviesFunctions = {
    val movieReviews = MovieIO.getMovieReviews(inputFilePath)
    val movies = batchMovieReviews(movieReviews)
    return new MoviesFunctions(movies)
  }*/

  def batchMovieReviews(reviews: RDD[MovieReview]): RDD[Movie] = {
    val reviewsGrouped: RDD[(String, Iterable[MovieReview])] = reviews.groupBy(_.movieId)
    reviewsGrouped.map {
      case (id, movieReviews) => new Movie(id, movieReviews.toVector)
    }
  }
}
