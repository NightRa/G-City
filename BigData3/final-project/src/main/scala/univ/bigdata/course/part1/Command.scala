package univ.bigdata.course.part1

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.Movie
import MoviesFunctions._
import univ.bigdata.course.util.Doubles

/** A CommandTask represents everything that needs to be done for a command task: input file, output file, and a list of commands. */
case class CommandsTask(reviewsFileName: String, outputFile: String, commands: Vector[Command])

/** All the Commands extend Command, supply a name and execution: What to output given the Movies RDD. These call the actual functions. */
sealed trait Command {
  def commandName: String
  final def execute(movies: RDD[Movie]): String = commandName + "\n" + exec(movies) + "\n"
  protected def exec(movies: RDD[Movie]): String
}

// The Command classes
case object TotalMoviesAverageScore extends Command {
  override def commandName: String = "totalMoviesAverageScore"
  override def exec(movies: RDD[Movie]): String =
    s"Total average: ${totalMoviesAverageScore(movies)}"
}
case class TotalMovieAverage(productID: String) extends Command {
  override def commandName: String = s"totalMovieAverage $productID"
  override protected def exec(movies: RDD[Movie]): String =
    s"Movies $productID average is ${Doubles.round(totalMovieAverage(movies, productID))}"
}
case class GetTopKMoviesAverage(topK: Int) extends Command {
  override def commandName: String = s"getTopKMoviesAverage $topK"
  override protected def exec(movies: RDD[Movie]): String = getTopKMoviesAverage(movies, topK).mkString("\n")
}
case object MovieWithHighestAverage extends Command {
  override def commandName: String = "movieWithHighestAverage"
  override protected def exec(movies: RDD[Movie]): String =
    s"The movie with highest average:  ${movieWithHighestAverage(movies).getOrElse("")}"
    // Notice the double space here. This is how it's in the example output.
}
case object MostReviewedProduct extends Command {
  override def commandName: String = "mostReviewedProduct"
  override protected def exec(movies: RDD[Movie]): String = s"The most reviewed movie product id is ${mostReviewedProduct(movies).movieId}"
}
case class ReviewCountPerMovieTopKMovies(topK: Int) extends Command {
  override def commandName: String = s"reviewCountPerMovieTopKMovies $topK"
  override protected def exec(movies: RDD[Movie]): String = {
    reviewCountPerMovieTopKMovies(movies, topK).map {
      case (id, numReviews) => s"Movie product id = [$id], reviews count [$numReviews]."
    }.mkString("\n")
  }
}
case class MostPopularMovieReviewedByKUsers(numOfUsers: Int) extends Command {
  override def commandName: String = s"mostPopularMovieReviewedByKUsers $numOfUsers"
  override protected def exec(movies: RDD[Movie]): String =
    s"Most popular movie with highest average score, reviewed by at least $numOfUsers users ${mostPopularMovieReviewedByKUsers(movies, numOfUsers).map(_.movieId).getOrElse("")}"
}
case class MoviesReviewWordsCount(topK: Int) extends Command {
  override def commandName: String = s"moviesReviewWordsCount $topK"
  override protected def exec(movies: RDD[Movie]): String = {
    moviesReviewWordsCount(movies, topK).map {
      case (word, namOccurrences) => s"Word = [$word], number of occurrences [$namOccurrences]."
    }.mkString("\n")
  }
}
case class TopYMoviesReviewTopXWordsCount(topMovies: Int, topWords: Int) extends Command {
  override def commandName: String = s"topYMoviesReviewTopXWordsCount $topMovies $topWords"
  override protected def exec(movies: RDD[Movie]): String = {
    topYMoviesReviewTopXWordsCount(movies, topMovies, topWords).map {
      case (word, namOccurrences) => s"Word = [$word], number of occurrences [$namOccurrences]."
    }.mkString("\n")
  }

}
case class TopKHelpfullUsers(topK: Int) extends Command {
  override def commandName: String = s"topKHelpfullUsers $topK"
  override protected def exec(movies: RDD[Movie]): String = {
    topKHelpfullUsers(movies, topK).map {
      case (userID, helpfulness) =>  s"User id = [$userID], helpfulness [${Doubles.round(helpfulness)}]."
    }.mkString("\n")
  }
}
case object MoviesCount extends Command {
  override def commandName: String = "moviesCount"
  override protected def exec(movies: RDD[Movie]): String =
    s"Total number of distinct movies reviewed [${moviesCount(movies)}]."
}
