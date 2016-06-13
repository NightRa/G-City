package univ.bigdata.course.part1

import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.{Helpfulness, Movie, MovieReview}
import univ.bigdata.course.util.Doubles._

object MoviesFunctions {
  // Can have as input whatever you need.
  // Examples:
  //   movies: RDD[Movie]
  //   moviesById: RDD[(String, Movie)]

  def totalMoviesAverageScore(movies: RDD[Movie]): Double = {
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
    val numMovies = movies.count()
    val topK: Int = Math.ceil((1 - (percent / 100)) * numMovies).toInt
    getTopKMoviesAverage(movies, topK)
    // TODO: Notice we do 2 queries over the data here! Kind of bad..
  }

  def mostReviewedProduct(movies: RDD[Movie]): Movie = {
    val order = Ordering.by((movie: Movie) => movie.movieReviews.size)
    movies.max()(order)
  }

  def topKMoviesByNumReviews(movies: RDD[Movie], topK: Int): Vector[Movie] = {
    implicit val ordering = Ordering.by((movie: Movie) => (-movie.movieReviews.size, movie.movieId))
    movies.sortBy(identity).take(topK).toVector
  }

  def reviewCountPerMovieTopKMovies(movies: RDD[Movie], topK: Int): Vector[(String, Long)] = {
    topKMoviesByNumReviews(movies, topK).map(movie => (movie.movieId, movie.numReviews))
  }

  def mostPopularMovieReviewedByKUsers(movies: RDD[Movie], numOfUsers: Int): Movie = {
    val reviewedMovies: RDD[Movie] = movies.filter(_.movieReviews.size >= numOfUsers)
    movieWithHighestAverage(reviewedMovies)
  }

  def moviesReviewWordsCount(movies: RDD[Movie], topK: Int): Array[(String, Long)] = {
    val summaries: RDD[String] = movies.flatMap(_.movieReviews.map(_.review))

    val words: RDD[String] = summaries.flatMap(_.split(" "))

    val wordsCounts: RDD[(String, Long)] = words.map(word => (word, 1L)).reduceByKey(_ + _)

    val orderByFreq: Ordering[(String, Long)] = Ordering.by(_._2)
    val orderByFreqDescending = orderByFreq.reverse
    val orderByLex: Ordering[(String, Long)] = Ordering.by(_._1)

    implicit val order: Ordering[(String, Long)] = Ordering.by {
      case (word, namOccurrences) => (-namOccurrences, word)
    }

    wordsCounts.sortBy(identity).take(topK)
  }


  def topKHelpfullUsers(movies: RDD[Movie], topK: Int): Array[(String, Double)] = {
    val reviewsByReviewer : RDD[(String, Iterable[MovieReview])] =
      movies.flatMap(movie => movie.movieReviews)
        .groupBy(review => review.userId)
    val usersHelpfulness : RDD[(String, Double)] =
      reviewsByReviewer
        .mapValues(reviews => reviews.map(_.helpfulness))
        .mapValues(_.fold(new Helpfulness(0,0))(Helpfulness.combine))
        .mapValues(_.helpfulnessRatio())
        .filter{case (userId, maybeHelpfulness) => maybeHelpfulness.isPresent}
        .mapValues(_.get())

    implicit val order : Ordering[(String, Double)] = Ordering.by{case (userId, helpfulnessRatio) => (-helpfulnessRatio, userId)}
    usersHelpfulness.sortBy(identity).take(topK)
  }
  def topYMoviewsReviewTopXWordsCount(movies: RDD[Movie], topMovies: Int, topWords: Int): Array[(String, Long)] = {
    val topYMovies: RDD[Movie] = SparkMain.sc.parallelize(topKMoviesByNumReviews(movies, topMovies))
    moviesReviewWordsCount(topYMovies, topWords)
  }

  def moviesCount(movies: RDD[Movie]): Long = {
    movies.count()
  }

}
