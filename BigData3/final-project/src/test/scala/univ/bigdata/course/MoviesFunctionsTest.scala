package univ.bigdata.course

import java.util.{Arrays, Collections}

import org.junit.{Assert, Test}
import univ.bigdata.course.part1.MoviesFunctions
import univ.bigdata.course.part1.movie.{Helpfulness, Movie}
import TestBuilders._
import MoviesFunctions._

class MoviesFunctionsTest {
  @Test
  def totalMoviesAvgTest1() {
    val movie1 = scoredMovie("movie1", 1, 2, 3, 4, 5)
    val movie2 = scoredMovie("movie2", 5, 3, 5, 3)
    val movie3 = scoredMovie("movie3", 5, 5, 5, 5)
    val movies = moviesRDD(movie1, movie2, movie3)
    val totalAvg: Double = totalMoviesAverageScore(movies)
    Assert.assertEquals(3.92308, totalAvg, 1e-15)
  }

  @Test
  def testGroupMoviesById() {
    /*val movie1: Movie = namedMovie("movie1")
    val movie2: Movie = namedMovie("movie2")
    val movie3: Movie = namedMovie("movie3")
    val moviesByName = groupMoviesById(movie1, movie2, movie3)
    val expected = new util.HashMap[String, Movie]
    expected.put("movie1", movie1)
    expected.put("movie2", movie2)
    expected.put("movie3", movie3)
    Assert.assertEquals(expected, moviesByName)*/
  }

  @Test
  def testGroupMoviesByIdEmpty() {
    /*val moviesByName = groupMoviesById(Collections.emptyList)
    val expected = new util.HashMap[String, Movie]
    Assert.assertEquals(expected, moviesByName)*/
  }

  @Test
  def testMoviesCountNoList() {
    Assert.assertEquals(0, moviesCount(SparkMain.sc.emptyRDD))
  }

  @Test
  def testMoviesCountFourMovies() {
    val movies = moviesRDD(
      namedMovie("movie1"), namedMovie("movie2"), namedMovie("movie3"), namedMovie("movie4"))
    Assert.assertEquals(4, moviesCount(movies))
  }

  @Test
  def testMostReviewedProduct17() {
    val movie1 = movieManyReviews("movie1", 6)
    val movie2 = movieManyReviews("movie2", 17)
    val movie3 = movieManyReviews("movie3", 15)
    val movie4 = movieManyReviews("movie4", 8)
    val movies = moviesRDD(movie1, movie2, movie3, movie4)
    Assert.assertEquals(movie2, mostReviewedProduct(movies))
  }

  @Test
  def testMostReviewedProduct56() {
    val movies = moviesRDD(
      movieManyReviews("movie1", 22),
      movieManyReviews("movie2", 55),
      movieManyReviews("movie3", 56),
      movieManyReviews("movie4", 22),
      movieManyReviews("movie5", 1)
    )
    Assert.assertEquals("movie3", mostReviewedProduct(movies))
  }

  @Test
  def testMovieWithHighestAverage() {
    val movies =
      moviesRDD(
        scoredMovie("Tony", 10, 10, 10, 10, 5),
        scoredMovie("Yuval", 10, 10, 10, 10, 10, 10),
        scoredMovie("Ilan", 10, 10, 10, 10),
        scoredMovie("Charlie", 5, 5, 5)
      )
    val expected = scoredMovie("Ilan", 10, 10, 10, 10)
    Assert.assertEquals(expected, movieWithHighestAverage(movies))
  }

  val moviesNames = moviesRDD(
    scoredMovie("Godik", 9, 2, 10, 5, 4),
    scoredMovie("Mubariky", 4, 3, 10, 7),
    scoredMovie("Hankin", 8, 3, 1),
    scoredMovie("Tannous", 5, 3),
    scoredMovie("Alfassi", 1, 3, 9, 7)
  )

  @Test
  def testMovieTopKMovieAverage() {
    val actual = getTopKMoviesAverage(moviesNames, 2)
    val expected = Vector(
      scoredMovie("Godik", 9, 2, 10, 5, 4),
      scoredMovie("Mubariky", 4, 3, 10, 7)
    )


    Assert.assertEquals(expected, actual)
  }

  val moviesHelpers = moviesRDD(
    createMovieHelp("movie1", "user1", new Helpfulness(1, 3), "user2", new Helpfulness(2, 3)),
    createMovieHelp("movie2", "user3", new Helpfulness(0, 5), "user4", new Helpfulness(3, 9)),
    createMovieHelp("movie3", "user5", new Helpfulness(5, 6), "user6", new Helpfulness(9, 10))
  )


  @Test
  def testTopKHelpfullUsers1() {
    Assert.fail("Need to implement topKHelpfullUsers!")
    /*val helpfuls: Map[String, Double] = topKHelpfullUsers(moviesHelpers, 3)
    val expected = Map("user2" -> 1.0, "user5" -> 2.0, "user6" -> 3.0)
    Assert.assertEquals(helpfuls, expected)*/
  }

  @Test
  def testTopKHelpfullUsers2() {
    Assert.fail("Need to implement topKHelpfullUsers!")
    /*val helpfuls = topKHelpfullUsers(moviesHelpers, 8)
    val expected = Map(
      "user1" -> 4.0,
      "user2" -> 1.0,
      "user3" -> 2.0,
      "user4" -> 3.0,
      "user5" -> 5.0,
      "user6" -> 5.0
    )
    Assert.assertEquals(expected, helpfuls)*/
  }

  @Test
  def testMovieTopKMovieAverage2() {
    val expected = Vector(scoredMovie("Godik", 9, 2, 10, 5, 4))
    Assert.assertEquals(expected, getTopKMoviesAverage(moviesNames, 1))
  }

  @Test
  def testMoviesReviewWordsCount() {
    val movies = moviesRDD(
      createMovieSummary("movie1", "Hello, how are you? my my my my my my. nice.. good good", "Ilan Godik is is is nice"),
      createMovieSummary("movie2", "Tony tannous and artem berger are learning OOP", "Germany has went out of the door in order to"),
      createMovieSummary("movie3", "When I first met becca, I was all what the hecka", "Susan boil's toothbrush")
    )

    val wordsCount = moviesReviewWordsCount(movies, 20)
    Assert.assertEquals(20, wordsCount.keySet.size)
    // Assert.assertEquals("my", iterator.next.getKey)
    // Assert.assertEquals(3L, iterator.next.getValue)
  }

}
