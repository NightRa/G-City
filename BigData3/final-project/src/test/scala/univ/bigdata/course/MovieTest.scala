package univ.bigdata.course

import org.junit.{Assert, Test}
import TestBuilders._
import univ.bigdata.course.part1.MoviesFunctions

class MovieTest {
  @Test
  def testMovieAvg() {
    val movie1 = scoredMovie("movie1", 1, 5, 3, 4, 1, 1)
    val expectedAvg: Double = (1 + 5 + 3 + 4 + 1 + 1).toDouble / 6
    Assert.assertEquals(expectedAvg, movie1.avgScore, 1e-15)
  }

  @Test
  def testMovieAvgOneReview() {
    val movieNoReviews = scoredMovie("movie2", 3)
    val expectedAvg: Double = 3
    Assert.assertEquals(expectedAvg, movieNoReviews.avgScore, 1e-15)
  }

  @Test
  def testMostPopularMovieReviewedByKUsers() {
    val movie1  = scoredMovie("Yuval", 1, 5, 3, 3, 5, 9, 10)
    val movie2 = scoredMovie("Tony", 0, 0, 4, 3, 1)
    val movies = moviesRDD(movie1, movie2)
    val id: String = MoviesFunctions.mostPopularMovieReviewedByKUsers(movies, 6)
    Assert.assertEquals(id, movie1.movieId)
  }

  @Test
  def testMoviePercentile() {
    val movie1 = scoredMovie("AA", 5, 7, 9, 8, 9, 9, 9)
    val movie2 = scoredMovie("BB", 8)
    val movie3 = scoredMovie("cc", 10)
    val movie4 = scoredMovie("dd", 10)
    val movies = moviesRDD(movie1, movie2, movie3, movie4)
    val t = MoviesFunctions.getMoviesPercentile(movies, 75)
    Assert.assertEquals(movie3.movieId, t(0).movieId)
  }

}