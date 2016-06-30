package univ.bigdata.course.part1

import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.{Helpfulness, Movie, MovieReview}
import univ.bigdata.course.util.Doubles._

object MoviesFunctions {
  // These are ported implementations from HW1 to Spark.

  /**
    * Note: All the following functions get the movies as
    * parameter (type - RDD[Movie])
    */

  /**
    * Method which calculates total scores average for all movies.
    *
    * @return the average
    */
  def totalMoviesAverageScore(movies: RDD[Movie]): Double = {
    round(movies.flatMap(_.movieReviews.map(_.score)).mean())
  }

  /**
    * Given a movieId, find that movie and return it's average score.
    *
    * @param productId id of the movie to calculate the average score of.
    * @return the movie's average, or -1 if movie not found.
    */
  def totalMovieAverage(movies: RDD[Movie], productId: String): Double = {
    val maybeMovie: Array[Movie] = movies.filter(_.movieId == productId).take(1)
    if (maybeMovie.isEmpty) -1
    else
      round(maybeMovie.head.avgScore)
  }

  /**
    * For each movie calculates it's average score. List should be sorted
    * by average score in decreasing order and in case of same average tie
    * should be broken by natural order of product id. Only top k movies
    * with highest average should be returned
    *
    * @param topK - number of top movies to return
    * @return - list (type - Vector[Movie]) of movies where each Movie includes it's average
    */
  def getTopKMoviesAverage(movies: RDD[Movie], topK: Int): Vector[Movie] = {
    val ordering = Ordering.by((movie: Movie) => (-movie.avgScore, movie.movieId))
    movies.takeOrdered(topK)(ordering).toVector
  }

  /**
    * Finds movie with the highest average score among all available movies.
    * If more than one movie has same average score, then movie with lowest
    * product id ordered lexicographically.
    *
    * @return - the movies record
    */
  def movieWithHighestAverage(movies: RDD[Movie]): Option[Movie] = {
    getTopKMoviesAverage(movies, 1).headOption
  }

  /**
    * Returns a list of movies which has average of given percentile.
    * List should be sorted according the average score of movie and in case there
    * are more than one movie with same average score, sort by product id
    * lexicographically in natural order.
    *
    * @param percent - the percentile, value in range between [0..100]
    * @return - movies list (type Vector[Movie])
    */
  def getMoviesPercentile(movies: RDD[Movie], percent: Double): Vector[Movie] = {
    val numMovies = movies.count()
    val topK: Int = Math.ceil((1 - (percent / 100)) * numMovies).toInt
    getTopKMoviesAverage(movies, topK)
  }

  /**
    * @return - the most reviewed movie among all movies
    */
  def mostReviewedProduct(movies: RDD[Movie]): Movie = {
    val order = Ordering.by((movie: Movie) => movie.movieReviews.size)
    movies.max()(order) // May throw, hopefully we won't get empty datasets.
  }

  def topKMoviesByNumReviews(movies: RDD[Movie], topK: Int): Vector[Movie] = {
    implicit val ordering = Ordering.by((movie: Movie) => (-movie.movieReviews.size, movie.movieId))
    movies.takeOrdered(topK)(ordering).toVector
  }

  /**
    * Computes reviews count per movie, sorted by reviews count in decreasing order, for movies
    * with same amount of review should be ordered by product id. Returns only top k movies with
    * highest review count.
    *
    * @return - returns map (type - Vector[(String, Long)]) with movies product id and
    *         the count of over all reviews assigned to it.
    */
  def reviewCountPerMovieTopKMovies(movies: RDD[Movie], topK: Int): Vector[(String, Long)] = {
    topKMoviesByNumReviews(movies, topK).map(movie => (movie.movieId, movie.numReviews))
  }

  /**
    * Computes most popular movie which has been reviewed by at least
    * numOfUsers (provided as parameter).
    *
    * @param numOfUsers - limit of minimum users which reviewed the movie
    * @return - movie which got highest count of reviews
    */
  def mostPopularMovieReviewedByKUsers(movies: RDD[Movie], numOfUsers: Int): Option[Movie] = {
    val reviewedMovies: RDD[Movie] = movies.filter(_.movieReviews.size >= numOfUsers)
    movieWithHighestAverage(reviewedMovies)
  }

  /**
    * Compute map of words count for top K words
    *
    * @param topK - top k number
    * @return - map (type - Array[(String, Long)]) where key is the word and value is the number of occurrences
    *         of this word in the reviews summary, map ordered by words count in decreasing order.
    */
  def moviesReviewWordsCount(movies: RDD[Movie], topK: Int): Array[(String, Long)] = {
    val summaries: RDD[String] = movies.flatMap(_.movieReviews.map(_.review))

    val words: RDD[String] = summaries.flatMap(_.split(" "))

    val wordsCounts: RDD[(String, Long)] = words.map(word => (word, 1L)).reduceByKey(_ + _)

    val order: Ordering[(String, Long)] = Ordering.by {
      case (word, numOccurrences) => (-numOccurrences, word)
    }

    wordsCounts.takeOrdered(topK)(order)
  }

  /**
    * Compute top k most helpful users that provided reviews for movies
    *
    * @param topK - number of users to return
    * @return - map of users (type - Array[(String, Double)]) to number of reviews they made. Map ordered by number of reviews
    *         in decreasing order.
    */
  def topKHelpfullUsers(movies: RDD[Movie], topK: Int): Array[(String, Double)] = {
    val reviewsByReviewer: RDD[(String, Iterable[MovieReview])] =
      movies.flatMap(movie => movie.movieReviews)
        .groupBy(review => review.userId)
    val usersHelpfulness: RDD[(String, Double)] =
      reviewsByReviewer
        .mapValues(reviews => reviews.map(_.helpfulness))
        .mapValues(_.fold(new Helpfulness(0, 0))(Helpfulness.combine))
        .mapValues(_.helpfulnessRatio())
        .filter { case (userId, maybeHelpfulness) => maybeHelpfulness.isPresent }
        .mapValues(_.get())

    val order: Ordering[(String, Double)] = Ordering.by { case (userId, helpfulnessRatio) => (-helpfulnessRatio, userId) }
    usersHelpfulness.takeOrdered(topK)(order)
  }

  /**
    * Compute words count map for top Y most reviewed movies. Map includes top K
    * words.
    *
    * @param topMovies - number of top review movies
    * @param topWords  - number of top words to return
    * @return - map (type - Array[(String, Long)]) of words to count, ordered by count in decreasing order.
    */
  def topYMoviesReviewTopXWordsCount(movies: RDD[Movie], topMovies: Int, topWords: Int): Array[(String, Long)] = {
    val topYMovies: RDD[Movie] = SparkMain.sc.parallelize(topKMoviesByNumReviews(movies, topMovies))
    moviesReviewWordsCount(topYMovies, topWords)
  }

  /**
    * Total movies count
    */
  def moviesCount(movies: RDD[Movie]): Long = {
    movies.count()
  }

}
