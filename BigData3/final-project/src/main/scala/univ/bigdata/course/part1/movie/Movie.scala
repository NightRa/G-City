package univ.bigdata.course.part1.movie

import univ.bigdata.course.util.Doubles._

case class Movie(movieId: String, movieReviews: Vector[MovieReview]) {
  /**
    * The reviews for the given movie.
    * Invariant: all the reviews should have the @movieId field equal to the Movie's @movieId field.
    * Invariant: Must be non empty!
    **/

  lazy val avgScore = movieReviews.view.map(_.score).sum / movieReviews.size

  override def toString: String =
    "Movie{" +
      "productId='" + movieId + '\'' +
      ", score=" + round(avgScore) + '}'
}
