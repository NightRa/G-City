package univ.bigdata.course.part1

sealed trait Command
case object TotalMoviesAverageScore
case class TotalMovieAverage(productID: String)
case class GetTopKMoviesAverage(topK: Int)
case object MovieWithHighestAverage
case object MostReviewedProduct
case class ReviewCountPerMovieTopKMovies(topK: Int)
case class MostPopularMovieReviewedByKUsers(numOfUsers: Int)
case class MoviesReviewWordsCount(topK: Int)
case class TopYMoviesReviewTopXWordsCount(topMovies: Int, topWords: Int)
case class TopKHelpfulUsers(k: Int)
case object MoviesCount
