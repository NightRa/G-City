package univ.bigdata.course.part1

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.Movie
import univ.bigdata.course.util.Doubles._

import scala.reflect.ClassTag
import scalaz.Order
import scalaz.std.int
import scalaz.syntax.semigroup._
import Math._

import org.apache.spark.api.java

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

  def getMoviesPercentile(movies:RDD[Movie], percent:Double):Array[Movie] = {
            val topK:Int = Math.ceil(1 - (percent / 100) * movies.count()).toInt
            getTopKMoviesAverage(movies, topK)
        }
}
