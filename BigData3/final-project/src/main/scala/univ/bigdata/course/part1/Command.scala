package univ.bigdata.course.part1

case class CommandsTask(reviewsFileName: String, outputFile: String, commands: Vector[Command])

sealed trait Command
case object TotalMoviesAverageScore extends Command
case class TotalMovieAverage(productID: String) extends Command
case class GetTopKMoviesAverage(topK: Int) extends Command
case object MovieWithHighestAverage extends Command
case object MostReviewedProduct extends Command
case class ReviewCountPerMovieTopKMovies(topK: Int) extends Command
case class MostPopularMovieReviewedByKUsers(numOfUsers: Int) extends Command
case class MoviesReviewWordsCount(topK: Int) extends Command
case class TopYMoviesReviewTopXWordsCount(topMovies: Int, topWords: Int) extends Command
case class TopKHelpfulUsers(k: Int) extends Command
case object MoviesCount extends Command
