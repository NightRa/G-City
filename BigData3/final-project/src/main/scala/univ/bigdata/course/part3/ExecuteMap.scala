package univ.bigdata.course.part3

import java.nio.file.{Files, Path, Paths}

import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.{Movie, MovieReview}
import univ.bigdata.course.part1.preprocessing.MovieIO


object ExecuteMap {

  type UserId = String
  type Rank = Int
  def execute (moviesTrainPath : String, moviesTestPath : String) = {
    // Check whether files' pathes exist

    if (!Files.exists(Paths.get(moviesTrainPath)))
      sys.error(s"Train file $moviesTrainPath doesn't exist")
    if (!Files.exists(Paths.get(moviesTestPath)))
      sys.error(s"Test file $moviesTestPath doesn't exist")

    // ------------------------------------------------------------
    // Read movies

    //val trainDataSet = MovieIO.readMovies(moviesTrainPath)
    val userIds: RDD[(UserId, Iterable[MovieReview])] = MovieIO.getMovieReviews(moviesTestPath).groupBy(_.userId)
    val ranker : RDD[Vector[Rank]] =  ??? // createRanker (userIds, )

    val mapResult = Map.calcMap(ranker)
    println(mapResult)
  }
}
