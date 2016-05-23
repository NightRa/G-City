package univ.bigdata.course.part1.movie

import org.apache.spark.rdd.RDD
import univ.bigdata.course.util.Lazy
import org.apache.spark.SparkContext._
import univ.bigdata.course.util.Doubles._

case class Movie(movieId: String, private val _movieReviews: RDD[MovieReview]) {
  /**
    * The reviews for the given movie.
    * Invariant: all the reviews should have the @movieId field equal to the Movie's @movieId field.
    * Invariant: Must be non empty!
    **/
  val movieReviews = _movieReviews.cache()

  lazy val avgScore = movieReviews.map(_.score).mean()

  override def toString: String =
    "Movie{" +
      "productId='" + movieId + '\'' +
      ", score=" + round(avgScore) + '}'
}
