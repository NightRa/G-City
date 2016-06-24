package univ.bigdata.course.part3

import java.io.{BufferedWriter, FileWriter}
import java.nio.file.{Files, Paths}

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.{LongMatrixFactorizationModel, Ranking}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.SparkMain
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO
import univ.bigdata.course.part2.Recommendation._
import univ.bigdata.course.part2.TestedUser

object ExecuteMap {

  /*val pathName = "C:\\Users\\Yuval\\Desktop\\Output.txt"
  val writer = new FileWriter(pathName, true);
  def safePrint(value : String) = {
    println(value)
    val path = Paths.get(pathName)
    if (!Files.exists(path))
      Files.createFile(path)
    writer.write(value + '\n')
    writer.flush()
  }*/

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

    //val reviews = MovieIO.getMovieReviews(moviesTrainPath)
    //val testReviews = MovieIO.getMovieReviews(moviesTestPath)
    val trainRatio = 0.6
    val allReviews = MovieIO.getMovieReviews(moviesTrainPath)
    val allReviewsSorted = allReviews.sortBy(_.timestamp).zipWithIndex()
    val amount = allReviews.count()
    val trainAmount = (amount * trainRatio).toLong
    val reviews = allReviewsSorted.filter(_._2 <= trainAmount).map(_._1)
    val testReviews = allReviewsSorted.filter(_._2 > trainAmount).map(_._1)
   // val Array(reviews, testReviews) = MovieIO.getMovieReviews(moviesTrainPath).randomSplit(Array(0.5, 0.5))
    val normalizedReviews = normalizeReviews(reviews).cache()
    val model = trainModel(normalizedReviews)

    val watchedMovies =
      reviews
        .groupBy(r => toID(r.userId))
        .mapValues(_.map(r => toID(r.movieId)).toSeq)
    val moviesToTest =
      testReviews.groupBy(r => toID(r.userId))
        .mapValues(_.filter(r => r.score >= 3.0))
        .mapValues(_.map(r => toID(r.movieId)).toSeq)
    val testedUsers =
      watchedMovies
        .join(moviesToTest)
        .map{ case (userId,(seenMovies,unseenMovies)) => TestedUser(userId,seenMovies,unseenMovies)}

    val ranks: Iterator[Array[Rank]] = relevantRankLists(model, testedUsers)

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

  def userRanks(model: LongMatrixFactorizationModel, user: TestedUser): Array[Rank] = {
    val userFeaturesO = model.userFeatures.lookup(user.userId).headOption
    userFeaturesO match {
      case None => Array.empty
      case Some(userFeatures) =>
          LongMatrixFactorizationModel
            .allRecommendations(userFeatures, model.productFeatures)
            .filter{case (movieID, rating) => !user.moviesSeen.contains(movieID)} // Crusial! -- Ilan's mistake :O
            .zipWithIndex()
            .map {
            case ((movieID, rating), ranking) => Ranking(user.userId, movieID, rating.toFloat, ranking.toInt)
          }.filter(ranking => user.unknownRecommendedMovies.contains(ranking.movie))
            .map(_.ranking)
            .collect()
            .sorted
    }
  }

  def relevantRankLists(model: LongMatrixFactorizationModel, users : RDD[TestedUser]): Iterator[Array[Rank]] = {
    users
      .toLocalIterator      // is that OK? If not throws an exception: RDD inside RDD
      .map(testedUser => userRanks(model, testedUser))
  }
}
