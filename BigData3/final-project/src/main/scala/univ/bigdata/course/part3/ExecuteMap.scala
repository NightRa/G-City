package univ.bigdata.course.part3

import java.nio.file.{Files, Paths}

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.{LongMatrixFactorizationModel, Ranking}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO
import univ.bigdata.course.part2.Recommendation._
import univ.bigdata.course.part2.TestedUser

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

    val reviews = normalizeReviews(MovieIO.getMovieReviews(moviesTrainPath)).cache()
    val testReviews = MovieIO.getMovieReviews(moviesTestPath).cache() // Don't normalize, we filter score >= 3
    // val trainRatio = 0.9
    // val allReviews = MovieIO.getMovieReviewsRaw(moviesTrainPath)
    // val allReviewsSorted = allReviews.sortBy(_.timestamp).zipWithIndex()
    // val amount = allReviews.count()
    // val trainAmount = (amount * trainRatio).toLong
    // val reviews = allReviewsSorted.filter(_._2 <= trainAmount).map(_._1)
    // val testReviews = allReviewsSorted.filter(_._2 > trainAmount).map(_._1)
    // // val Array(reviews, testReviews) = MovieIO.getMovieReviews(moviesTrainPath).randomSplit(Array(0.5, 0.5))
    val model = trainModel(reviews)

    // userID => movieIDs
    val watchedMovies: RDD[(Long, Seq[Long])] =
      reviews
        .groupBy(r => toID(r.userId))
        .mapValues(_.map(r => toID(r.movieId)).toSeq)
    // userID => movieIDs w/ >=3 score
    val moviesToTest: RDD[(Long, Seq[Long])] =
      testReviews.groupBy(r => toID(r.userId))              // assumption: number of movies a user sees is << 2^32
        .mapValues(_.filter(_.score >= 3.0).map(r => toID(r.movieId)).toSeq) // the user should have liked the movie
        .filter(_._2.nonEmpty) // Users that still have some movies they liked, which we can check.
    val testedUsers: RDD[TestedUser] =
      watchedMovies
        .rightOuterJoin(moviesToTest)
        .map { case (userId, (seenMovies, unseenMovies)) => TestedUser(userId, seenMovies, unseenMovies) }

    val ranks: Iterator[Array[Rank]] = relevantRankLists(model, testedUsers)

    val mapResult: Double = Map.calcMap(ranks)
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

  def userRanks(model: LongMatrixFactorizationModel, user: TestedUser): Array[Rank] = {
    val userFeaturesO = model.userFeatures.lookup(user.userId).headOption
    val userFeatures = userFeaturesO.getOrElse(Array.fill(model.rank)(0d))
    // If user was not found - not in the original train set: Give vector comprised of 0's.
    // Because the matrix is normalized, this will give the average / most popular movies for all such users.

    LongMatrixFactorizationModel
      .allRecommendations(userFeatures, model.productFeatures)
      .filter { case (movieID, rating) => user.moviesSeen.fold(true)(!_.contains(movieID)) }
      // filter all the movies the user already watched, if they watched none, all are ok.
      .zipWithIndex()
      .map {
        case ((movieID, rating), ranking) => Ranking(user.userId, movieID, rating.toFloat, ranking.toInt)
      }.filter(ranking => user.unknownRecommendedMovies.contains(ranking.movie))
      .map(_.ranking)
      .collect()
      .sorted
  }

  def relevantRankLists(model: LongMatrixFactorizationModel, users: RDD[TestedUser]): Iterator[Array[Rank]] = {
    users               // number of tested users is small
      .toLocalIterator // is that OK? If not throws an exception: RDD inside RDD. It is ok! That's the point.
      .map(testedUser => userRanks(model, testedUser))
  }
}
