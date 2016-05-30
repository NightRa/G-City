package univ.bigdata.course

import java.util.stream.{Collectors, DoubleStream}

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.{Helpfulness, Movie, MovieReview}

object TestBuilders {
  def moviesRDD(movies: Movie*): RDD[Movie] = {
    SparkMain.sc.parallelize(movies)
  }

  def scoredReview(movieId: String, score: Double): MovieReview = {
    new MovieReview(movieId, "user", "user", new Helpfulness(1, 1), score, "", "", "")
  }

  def scoredMovie(movieId: String, score1: Double, scores: Double*): Movie = {
    val reviews = (score1 +: scores).map(score => scoredReview(movieId, score))

    new Movie(movieId, reviews.toVector)
  }

  def namedMovie(movieId: String): Movie = {
    scoredMovie(movieId, 3)
  }

  def movieManyReviews(movieId: String, numReviews: Int): Movie = {
    val movie = new MovieReview(movieId, "id", "name", new Helpfulness(1, 2), 0.5, "", "", "")
    val reviews = Vector.fill(numReviews)(movie)
    new Movie(movieId, reviews)
  }

  def createMovieHelp(movieId: String, user1: String, helpfulness1: Helpfulness, user2: String, helpfulness2: Helpfulness): Movie = {
    val movie1: MovieReview = new MovieReview(movieId, user1, user1, helpfulness1, 0, "", "", "")
    val movie2: MovieReview = new MovieReview(movieId, user2, user2, helpfulness2, 0, "", "", "")
    val reviews = Vector(movie1, movie2)
    new Movie(movieId, reviews)
  }

  def createMovieSummary(movieName: String, review1: String, review2: String): Movie = {
    val movie1: MovieReview = new MovieReview(movieName, "", "", null, 0, "", "", review1)
    val movie2: MovieReview = new MovieReview(movieName, "", "", null, 0, "", "", review2)
    val reviews = Vector(movie1, movie2)
    new Movie(movieName, reviews)
  }
}
