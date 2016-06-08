package univ.bigdata.course.part1

import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.Movie
import univ.bigdata.course.util.Doubles._
import univ.bigdata.course.util.OrderingCompose._

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

  def getTopKMoviesAverage(movies: RDD[Movie], topK: Int): Vector[Movie] = {
    implicit val ordering = Ordering.by((movie: Movie) => (-movie.avgScore, movie.movieId))
    movies.sortBy(identity).take(topK).toVector
  }

  def movieWithHighestAverage(movies: RDD[Movie]): Movie = {
    getTopKMoviesAverage(movies, 1)(0)
  }

  def getMoviesPercentile(movies: RDD[Movie], percent: Double): Vector[Movie] = {
    val topK: Int = Math.ceil(1 - (percent / 100) * movies.count()).toInt
    getTopKMoviesAverage(movies, topK)
  }

  def mostReviewedProduct(movies: RDD[Movie]): String = {
    val order = Ordering.by((movie: Movie) => movie.movieReviews.size)
    val topMovie: Movie = movies.max()(order)
    topMovie.movieId
  }

  def topKMoviesByNumReviews(movies: RDD[Movie], topK: Int): Vector[Movie] = {
    implicit val ordering = Ordering.by((movie: Movie) => (-movie.movieReviews.size, movie.movieId))
    movies.sortBy(identity).take(topK).toVector
  }

  def reviewCountPerMovieTopKMovies(movies: RDD[Movie], topK: Int): Map[String, Long] = {
    val topMovies = topKMoviesByNumReviews(movies, topK)
    topMovies.foldLeft(Map[String, Long]())((map, movie) => map ++ Map[String, Long]((movie.movieId, movie.movieReviews.size)))
  }

  def mostPopularMovieReviewedByKUsers(movies: RDD[Movie], numOfUsers: Int): String = {
    val reviewedMovies: RDD[Movie] = movies.filter(_.movieReviews.size >= numOfUsers)
    movieWithHighestAverage(reviewedMovies).movieId
  }

  def moviesReviewWordsCount(movies: RDD[Movie], topK: Int): Map[String, Long] = {
    val summaries: RDD[String] = movies.flatMap(_.movieReviews.map(_.review))

    val words: RDD[String] = summaries.flatMap(_.split(" "))

    val wordsCounts: RDD[(String, Long)] = words.map(word => (word, 1L)).reduceByKey(_ + _)

    val orderByFreq: Ordering[(String, Long)] = Ordering.by(_._2)
    val orderByFreqDescending = orderByFreq.reverse
    val orderByLex: Ordering[(String, Long)] = Ordering.by(_._1)

    implicit val ordering: Ordering[(String, Long)] = compose(orderByFreqDescending, orderByLex)

    wordsCounts.sortBy(identity).take(topK).toMap
  }

  def topYMoviewsReviewTopXWordsCount(movies: RDD[Movie], topMovies: Int, topWords: Int): Map[String, Long] = {
    val topYMovies: RDD[Movie] = SparkMain.sc.parallelize(topKMoviesByNumReviews(movies, topMovies))
    moviesReviewWordsCount(topYMovies, topWords)
  }

  def moviesCount(movies: RDD[Movie]): Long = {
    movies.count()
  }

}
