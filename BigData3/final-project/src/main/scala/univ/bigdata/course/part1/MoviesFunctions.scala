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
    Math.round(movies.flatMap(_.movieReviews.map(_.score)).mean())
  }

  def totalMovieAverage(moviesById: RDD[(String, Movie)], productId: String): Double = {
    val maybeMovie: Seq[Movie] = moviesById.lookup(productId)
    if (maybeMovie.isEmpty) -1
    else
      round(maybeMovie.head.avgScore)
  }

  def getTopKMoviesAverage(movies: RDD[Movie], topK: Int): Array[Movie] = {
    val order = Order.orderBy((movie: Movie) => movie.avgScore).reverseOrder |+| Order.orderBy((movie: Movie) => movie.movieId)
    movies.sortBy(identity)(order.toScalaOrdering, implicitly[ClassTag[Movie]]).take(topK)
  }

  def movieWithHighestAverage(movies: RDD[Movie]): Movie = {
    getTopKMoviesAverage(movies, 1)(0)
  }

  def getMoviesPercentile(movies: RDD[Movie], percent: Double): Array[Movie] = {
    val topK: Int = Math.ceil(1 - (percent / 100) * movies.count()).toInt
    getTopKMoviesAverage(movies, topK)
  }

  def mostReviewedProduct(movies: RDD[Movie]): String = {
    val order = Order.orderBy((movie: Movie) => movie.movieReviews.size)
    val topMovie: Movie = movies.max()(order.toScalaOrdering)
    topMovie.movieId
  }

  def topKMoviesByNumReviews(movies: RDD[Movie], topK: Int): Array[Movie] = {
    val order = Order.orderBy((movie: Movie) => movie.movieReviews.size).reverseOrder |+| Order.orderBy((movie: Movie) => movie.movieId)
    movies.sortBy(identity)(order.toScalaOrdering, implicitly[ClassTag[Movie]]).take(topK)
  }

  def reviewCountPerMovieTopKMovies(movies: RDD[Movie], topK: Int): Map[String, Long] = {
    val topMovies = topKMoviesByNumReviews(movies, topK)
    topMovies.foldLeft(Map[String, Long]())((map, movie) => map ++ Map[String, Long]((movie.movieId, movie.movieReviews.size)))
  }

  def mostPopularMovieReviewedByKUsers(movies:RDD[Movie], numOfUsers:Int):String = {
      val reviewedMovies:RDD[Movie]= movies.filter(_.movieReviews.size >= numOfUsers)
      movieWithHighestAverage(reviewedMovies).movieId
}
