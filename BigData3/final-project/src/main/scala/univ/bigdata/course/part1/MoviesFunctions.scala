package univ.bigdata.course.part1

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.Movie
import univ.bigdata.course.util.Doubles._

import scala.reflect.ClassTag
import scalaz.Order
import scalaz.syntax.semigroup._

object MoviesFunctions {
  // Can have as input whatever you need.
  // Examples:
  //   movies: RDD[Movie]
  //   moviesById: RDD[(String, Movie)]

  def totalMoviesAverageScore(movies: RDD[Movie]) = {
    round(movies.flatMap(_.movieReviews.map(_.score)).mean())
  }

  def totalMovieAverage(moviesById: RDD[(String, Movie)], productId: String): Double = {
    val maybeMovie: Seq[Movie] = moviesById.lookup(productId)
    if (maybeMovie.isEmpty) -1
    else
      round(maybeMovie.head.avgScore)
  }

  def getTopKMoviesAverage(movies: RDD[Movie], topK: Int): Array[Movie] = {
    val order = Order.orderBy((movie: Movie) => movie.avgScore) |+| Order.orderBy((movie: Movie) => movie.movieId)
    movies.sortBy(identity)(order.toScalaOrdering, implicitly[ClassTag[Movie]]).take(topK)
  }

  def movieWithHighestAverage(movies:RDD[Movie]):Movie = {
        getTopKMoviesAverage(movies,1)(0)
  }

}
