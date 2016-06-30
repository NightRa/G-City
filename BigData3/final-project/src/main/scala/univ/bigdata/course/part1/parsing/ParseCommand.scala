package univ.bigdata.course.part1.parsing

import univ.bigdata.course.part1._
import univ.bigdata.course.util.OptionUtils._

import scalaz.std.either._
import scalaz.std.vector._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._

object ParseCommand {
  // ParseCommand parses the commands file into a CommandTask
  // CommandTask contains a vector of commands, which is a sum of all the possible commands.

  def parseCommandsTask(lines: Seq[String]): Either[String, CommandsTask] = {
    lines.filter(_.nonEmpty) match {
      case Seq(inputFile, outputFile, commandsLines @ _*) =>
        for {
          commands <- parseLines(commandsLines)
          // Parse each line individually. This is the same as map.
        } yield CommandsTask(inputFile, outputFile, commands)
      case _ =>
        Left("Invalid file length")
    }
  }

  def parseLines (lines: Seq[String]) : Either[String, Vector[Command]] = {
    lines.toVector.traverseU(parse)
    // Note: It does compile. All ok. BTW, the U there is because the legendary scala issue that is fixed in 2.12
  }

  def assert(b: Boolean): Option[Unit] = if(b) Some(()) else None


  def parse(line : String) : Either[String, Command] = {
    val words = line.split(' ')
    if(words.length == 0) return Left(s"Bad command line: ${line}")
    val command = words(0)
    def getIntAt (index : Int) : Option[Int] = {
      maybeIndex(words, index).flatMap(maybeConvertToInt)
    }

    val opt = command match {
      case "mostReviewedProduct"              => (words.length == 1).option(MostReviewedProduct)
      case "totalMoviesAverageScore"          => (words.length == 1).option(TotalMoviesAverageScore)
      case "moviesCount"                      => (words.length == 1).option(MoviesCount)
      case "movieWithHighestAverage"          => (words.length == 1).option(MovieWithHighestAverage)
      case "totalMovieAverage"                =>
                                                  for {
                                                    _ <- assert(words.length == 2)
                                                    movieID <- maybeIndex(words, 1)
                                                  } yield TotalMovieAverage(movieID)
      case "reviewCountPerMovieTopKMovies"    =>
                                                  for {
                                                    _ <- assert(words.length == 2)
                                                    topK <- getIntAt(1)
                                                  } yield ReviewCountPerMovieTopKMovies(topK)
      case "moviesReviewWordsCount"           =>
                                                  for {
                                                    _ <- assert(words.length == 2)
                                                    topK <- getIntAt(1)
                                                  } yield MoviesReviewWordsCount(topK)
      case "getTopKMoviesAverage"             =>
                                                  for {
                                                    _ <- assert(words.length == 2)
                                                    topK <- getIntAt(1)
                                                  } yield GetTopKMoviesAverage(topK)
      case "mostPopularMovieReviewedByKUsers" =>
                                                  for {
                                                    _ <- assert(words.length == 2)
                                                    numOfUsers <- getIntAt(1)
                                                  } yield MostPopularMovieReviewedByKUsers(numOfUsers)
      case "topKHelpfullUsers"                =>
                                                  for {
                                                    _ <- assert(words.length == 2)
                                                    k <- getIntAt(1)
                                                  } yield TopKHelpfullUsers(k)
      case "topYMoviesReviewTopXWordsCount"  =>
                                                  for {
                                                    _ <- assert(words.length == 3)
                                                    topMovies <- getIntAt(1)
                                                    topWords <- getIntAt(2)
                                                  } yield TopYMoviesReviewTopXWordsCount(topMovies, topWords)
      case _                                  =>  None
    }

    // If failed (None), put the command on the left.
    opt.toRight(command)
  }
}
