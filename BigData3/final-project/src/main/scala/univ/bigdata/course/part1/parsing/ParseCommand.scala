package univ.bigdata.course.part1.parsing

import univ.bigdata.course.part1._
import univ.bigdata.course.util.OptionUtils._

import scalaz.std.option._
import scalaz.std.vector._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

object ParseCommand{
  def parseLines (lines : Array[String]) : Option[Vector[Command]] = {
    lines.toVector.traverse(parse)
  }

  // TODO: Unit tests
  def parse(line : String) : Option[Command] = {
    val words = line.split(' ')
    val command = words(0)
    def getIntAt (index : Int) : Option[Int] = {
      maybeIndex(words, index).flatMap(maybeConvertToInt)
    }

    command match {
      case "mostReviewedProduct"              => (words.length == 1).option(MostReviewedProduct)
      case "totalMoviesAverageScore"          => (words.length == 1).option(TotalMoviesAverageScore)
      case "moviesCount"                      => (words.length == 1).option(MoviesCount)
      case "totalMoviesAverageScore"          => (words.length == 1).option(TotalMoviesAverageScore)
      case "movieWithHighestAverage"          => (words.length == 1).option(MovieWithHighestAverage)
      case "reviewCountPerMovieTopKMovies"    =>
                                                  for {
                                                    _ <- (words.length == 2).option(())
                                                    topK <- getIntAt(1)
                                                  } yield ReviewCountPerMovieTopKMovies(topK)
      case "moviesReviewWordsCount"           =>
                                                  for {
                                                    _ <- (words.length == 2).option(())
                                                    topK <- getIntAt(1)
                                                  } yield MoviesReviewWordsCount(topK)
      case "getTopKMoviesAverage"             =>
                                                  for {
                                                    _ <- (words.length == 2).option(())
                                                    topK <- getIntAt(1)
                                                  } yield GetTopKMoviesAverage(topK)
      case "mostPopularMovieReviewedByKUsers" =>
                                                  for {
                                                    _ <- (words.length == 2).option(())
                                                    numOfUsers <- getIntAt(1)
                                                  } yield MostPopularMovieReviewedByKUsers(numOfUsers)
      case "topKHelpfullUsers"                =>
                                                  for {
                                                    _ <- (words.length == 2).option(())
                                                    k <- getIntAt(1)
                                                  } yield TopKHelpfulUsers(k)
      case "topYMoviewsReviewTopXWordsCount"  =>
                                                  for {
                                                    _ <- (words.length == 3).option(())
                                                    topMovies <- getIntAt(1)
                                                    topWords <- getIntAt(2)
                                                  } yield TopYMoviesReviewTopXWordsCount(topMovies, topWords)
      case _                                  =>  None
    }
  }
}
