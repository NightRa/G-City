package univ.bigdata.course.part3

import java.nio.file.{Files, Paths}

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.{LongMatrixFactorizationModel, Ranking}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO
import univ.bigdata.course.part2.Recommendation._

object ExecuteMap {

  type UserId = String
  type Rank = Int

  def execute(moviesTrainPath: String, moviesTestPath: String) = {
    // Check whether files' paths exist

    if (!Files.exists(Paths.get(moviesTrainPath)))
      sys.error(s"Train file $moviesTrainPath doesn't exist")
    if (!Files.exists(Paths.get(moviesTestPath)))
      sys.error(s"Test file $moviesTestPath doesn't exist")

    // ------------------------------------------------------------
    // Read movies


    val Array(reviews, testReviews) = MovieIO.getMovieReviews(moviesTrainPath).randomSplit(Array(0.9, 0.1))
    val normalizedReviews = normalizeReviews(reviews).cache()
    val model = trainModel(normalizedReviews)

    val usersMovies = /*MovieIO.getMovieReviews(moviesTestPath)*/testReviews.filter(_.score >= 3.0).map(r => (toID(r.userId), toID(r.movieId)))
    val ranks: Iterator[Array[Rank]] = relevantRankLists(model, usersMovies)



    val mapResult = Map.calcMap(ranks)
    println("============================================================\n\n\n\n")
    println(s"MAP Result: $mapResult")
    println("\n\n\n\n============================================================")

  }

  def trainModel(reviews: RDD[MovieReview]): LongMatrixFactorizationModel = {
    val ratings: RDD[Rating[Long]] = reviews.map(r => Rating(toID(r.userId), toID(r.movieId), r.score.toFloat)).cache()
    // Train using ALS
    val model: LongMatrixFactorizationModel = als(ratings)
    ratings.unpersist()
    model
  }

  def userRanks(user: Long, model: LongMatrixFactorizationModel, relevantMovies: Seq[Long]): Array[Rank] = {
    val userFeaturesO = model.userFeatures.lookup(user).headOption
    userFeaturesO match {
      case None => Array.empty
      case Some(userFeatures) =>
        val allRecommendations: RDD[Ranking] =
          LongMatrixFactorizationModel.allRecommendations(userFeatures, model.productFeatures).zipWithIndex().map {
            case ((movieID, rating), ranking) => Ranking(user, movieID, rating.toFloat, ranking.toInt)
          }
        val relevantRankings = allRecommendations.map(r => (r.movie, r)).join(SparkMain.sc.parallelize(relevantMovies).map((_, ()))).map {
          case (_user, (ranking, _)) => ranking
        }
        relevantRankings.map(_.ranking).collect().sorted
    }
  }

  def relevantRankLists(model: LongMatrixFactorizationModel, userMovies: RDD[(Long, Long)]): Iterator[Array[Int]] = {
    val userGroupedMovies = userMovies.groupByKey.cache()
    val users: Iterator[(Long, Iterable[Long])] = userGroupedMovies.toLocalIterator

    users.map {
      case (user, relevantMovies) => userRanks(user, model, relevantMovies.toSeq)
    }
  }
}
