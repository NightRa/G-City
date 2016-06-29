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
  def readMovies(inputFile: String): RDD[Movie] = {
    val reviews = getMovieReviews(inputFile)
    batchMovieReviews(reviews)
  }

  @throws[IOException]
  def getMovieReviews(inputFilePath: String): RDD[MovieReview] = {
    val reviewsLines = SparkMain.sc.textFile(inputFilePath)
    reviewsLines.map(MovieIOInternals.lineToReview)
  }

  def batchMovieReviews(reviews: RDD[MovieReview]): RDD[Movie] = {
    val reviewsGrouped: RDD[(String, Iterable[MovieReview])] = reviews.groupBy(_.movieId)
    reviewsGrouped.map {
      case (id, movieReviews) => new Movie(id, movieReviews.toVector)
    }
  }
}
