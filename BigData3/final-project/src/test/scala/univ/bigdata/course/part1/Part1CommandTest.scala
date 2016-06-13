package univ.bigdata.course.part1

import org.junit.{Assert, Test}
import univ.bigdata.course.part1.parsing.ParseCommand

class Part1CommandTest {

  // ************ mostReviewedProduct ************
  @Test
  def mostReviewedProductTest1() : Unit = {
    val actual = ParseCommand.parse("mostReviewedProduct")
    val expected = Right(MostReviewedProduct)
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostReviewedProductTest2() : Unit = {
    val actual = ParseCommand.parse("mostReviewedProduct12")
    val expected = Left("mostReviewedProduct12")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostReviewedProductTest3() : Unit = {
    val actual = ParseCommand.parse("MostReviewedProduct")
    val expected = Left("MostReviewedProduct")
    Assert.assertEquals(expected, actual)
  }

  // ************ mostPopularMovieReviewedByKUsers ************
  @Test
  def mostPopularMovieReviewedByKUsersTest1() : Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 10")
    val expected = Right(MostPopularMovieReviewedByKUsers(10))
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsersTest2() : Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 6775")
    val expected = Right(MostPopularMovieReviewedByKUsers(6775))
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsersTest3() : Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 11")
    val expected = Right(MostPopularMovieReviewedByKUsers(11))
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsersTest4() : Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 11 11 11")
    val expected = Left("mostPopularMovieReviewedByKUsers")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsersTest5() : Unit = {
    val actual = ParseCommand.parse("MosstPopularMovieReviewedByKUsers 14")
    val expected = Left("MosstPopularMovieReviewedByKUsers")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsersTest6() : Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers")
    val expected = Left("mostPopularMovieReviewedByKUsers")
    Assert.assertEquals(expected, actual)
  }

  // ************ topKHelpfullUsers ************
  @Test
  def topKHelpfullUsersTest1() : Unit = {
    val actual = ParseCommand.parse("topKHelpfullUsers 752")
    val expected = Right(TopKHelpfullUsers(752))
    Assert.assertEquals(expected, actual)
  }
  @Test
  def topKHelpfullUsersTest2() : Unit = {
    val actual = ParseCommand.parse("topKHelpfullUsers 23")
    val expected = Right(TopKHelpfullUsers(23))
    Assert.assertEquals(expected, actual)
  }
  @Test
  def topKHelpfullUsersTest3() : Unit = {
    val actual = ParseCommand.parse("topKHelpfullUserasds")
    val expected = Left("topKHelpfullUserasds")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def topKHelpfullUsersTest4() : Unit = {
    val actual = ParseCommand.parse("topKHelpfullUsers")
    val expected = Left("topKHelpfullUsers")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def topKHelpfullUsersTest5() : Unit = {
    val actual = ParseCommand.parse("23 topKHelpfullUsers")
    val expected = Left("23")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def topKHelpfullUsersTest6() : Unit = {
    val actual = ParseCommand.parse("topKHelpfullUsers 19 90")
    val expected = Left("topKHelpfullUsers")
    Assert.assertEquals(expected, actual)
  }

  // ************ moviesCount ************
  @Test
  def moviesCountTest1() : Unit = {
    val actual = ParseCommand.parse("moviesCount")
    val expected = Right(MoviesCount)
    Assert.assertEquals(expected, actual)
  }
  @Test
  def moviesCountTest2() : Unit = {
    val actual = ParseCommand.parse("moviesCount 87")
    val expected = Left("moviesCount")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def moviesCountTest3() : Unit = {
    val actual = ParseCommand.parse("MoviesCount")
    val expected = Left("MoviesCount")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def moviesCountTest4() : Unit = {
    val actual = ParseCommand.parse("Moviescount")
    val expected = Left("Moviescount")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def moviesCountTest5() : Unit = {
    val actual = ParseCommand.parse("10 Moviescount")
    val expected = Left("10")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def moviesCountTest6() : Unit = {
    val actual = ParseCommand.parse("Moviescount18")
    val expected = Left("Moviescount18")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def moviesCountTest7() : Unit = {
    val actual = ParseCommand.parse("Moviescount 18 9")
    val expected = Left("Moviescount")
    Assert.assertEquals(expected, actual)
  }

  // ************ totalMoviesAverage ************
  @Test
  def totalMoviesAverageScore1 (): Unit = {
    val expected = Right(TotalMoviesAverageScore)
    val actual = ParseCommand.parse("totalMoviesAverageScore")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def totalMoviesAverageScore2 (): Unit = {
    val actual = ParseCommand.parse("totalMoviesAverageScore12")
    val expected = Left("totalMoviesAverageScore12")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def totalMoviesAverageScore3 (): Unit = {
    val actual = ParseCommand.parse("totalMoviesAverageScore 45")
    val expected = Left("totalMoviesAverageScore")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def totalMoviesAverageScore4 (): Unit = {
    val actual = ParseCommand.parse("totalMoviewsAverageScore")
    val expected = Left("totalMoviewsAverageScore")
    Assert.assertEquals(expected, actual)
  }

  // ************ movieWithHighestAverage ************
  @Test
  def movieWithHighestAverage1(): Unit = {
    val expected = Right(MovieWithHighestAverage)
    val actual = ParseCommand.parse("movieWithHighestAverage")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def movieWithHighestAverage2(): Unit = {
    val actual = ParseCommand.parse("MovieWithHighestAverage 1")
    val expected = Left("MovieWithHighestAverage")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def movieWithHighestAverage3(): Unit = {
    val actual = ParseCommand.parse("MoviewWithHighestAverage")
    val expected = Left("MoviewWithHighestAverage")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def movieWithHighestAverage4(): Unit = {
    val actual = ParseCommand.parse("MovieMovieWithHighestAverage")
    val expected = Left("MovieMovieWithHighestAverage")
    Assert.assertEquals(expected, actual)
  }

  // ************ mostPopularMovieReviewedByKUsers ************
  @Test
  def mostPopularMovieReviewedByKUsers1(): Unit = {
    val expected = Right(MostPopularMovieReviewedByKUsers(20))
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 20")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsers2(): Unit = {
    val expected = Right(MostPopularMovieReviewedByKUsers(500))
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 500")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsers3(): Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers=20")
    val expected = Left("mostPopularMovieReviewedByKUsers=20")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsers4(): Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 20 00 9")
    val expected = Left("mostPopularMovieReviewedByKUsers")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsers5(): Unit = {
    val actual = ParseCommand.parse("mostPopularMovieReviewedByKUsers 1 2")
    val expected = Left("mostPopularMovieReviewedByKUsers")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsers6(): Unit = {
    val actual = ParseCommand.parse("MostPopularMovieReviewedByKUsers 1")
    val expected = Left("MostPopularMovieReviewedByKUsers")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostPopularMovieReviewedByKUsers7(): Unit = {
    val actual = ParseCommand.parse("mostPopularMoviewReviewedByKUsers 1")
    val expected = Left("mostPopularMoviewReviewedByKUsers")
    Assert.assertEquals(expected, actual)
  }

  // ************ mostReviewedProduct ************
  @Test
  def mostReviewedProduct1(): Unit = {
    val actual = ParseCommand.parse("mostReviewedProduct")
    val expected = Right(MostReviewedProduct)
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostReviewedProduct2(): Unit = {
    val actual = ParseCommand.parse("MostReviewedProduct")
    val expected = Left("MostReviewedProduct")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostReviewedProduct3(): Unit = {
    val actual = ParseCommand.parse("mostReviewwedProduct")
    val expected = Left("mostReviewwedProduct")
    Assert.assertEquals(expected, actual)
  }
  @Test
  def mostReviewedProduct4(): Unit = {
    val actual = ParseCommand.parse("mostReviewedProduct 33")
    val expected = Left("mostReviewedProduct")
    Assert.assertEquals(expected, actual)
  }

  // topYMoviesReviewTopXWordsCount
  @Test
  def topYMoviesReviewTopXWordsCount1(): Unit = {
    val actual = ParseCommand.parse("topYMoviesReviewTopXWordsCount 100 10")
    val expected = Right(TopYMoviesReviewTopXWordsCount(100, 10))
    Assert.assertEquals(expected, actual)
  }

  @Test
  def topYMoviesReviewTopXWordsCount2(): Unit = {
    val actual = ParseCommand.parse("topYMoviewsReviewTopXWordsCount")
    val expected = Left("topYMoviewsReviewTopXWordsCount")
    Assert.assertEquals(expected, actual)
  }

}
