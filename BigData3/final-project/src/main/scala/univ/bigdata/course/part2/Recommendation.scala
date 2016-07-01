package univ.bigdata.course.part2

import java.io.{FileOutputStream, PrintStream}
import java.security.MessageDigest

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.{LongALS, LongMatrixFactorizationModel}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO

case class Recommendation(userName: String, recommendations: Array[String]) {
  override def toString: String =
    s"Recommendations for $userName:" + recommendations.zipWithIndex.map {
      case (movieID, i) => s"${i + 1}. $movieID"
    }.mkString("\n", "\n", "\n======================================\n")
}

object Recommendation {
  val rank: Int = 75        // ALS constant  parameter
  val iterations: Int = 15  // ALS constant  parameter
  val numRecommendations: Int = 10

  def createRatings(reviews: RDD[MovieReview], movieIDs: RDD[(String, Long)], userIDs: RDD[(String, Long)]): RDD[Rating[Long]] = {
    val ratings =
      reviews.map(r => (r.movieId, r)).join(movieIDs).map {
        case (movieIDstr, (r, movieID)) => (r.userId, (r, movieID))
      }.join(userIDs).map {
        case (userIDstr, ((r, movieID), userID)) => Rating(userID, movieID, r.score.toFloat)
      }

    ratings
  }

  def als(ratings: RDD[Rating[Long]]): LongMatrixFactorizationModel = {
    // blocks, blocks, rank, iterations, lambda, false, 1.0
    val model = new LongALS().setRank(rank).setIterations(iterations).run(ratings)
    model
  }

  // Normalize reviews score to a zero average
  def normalizeReviews(reviews: RDD[MovieReview]): RDD[MovieReview] = {
    val normalizedMovies = {
      val rawMovies = MovieIO.batchMovieReviews(reviews)
      rawMovies.map { movie =>
        val avgScore = movie.avgScore
        movie.copy(movieReviews = movie.movieReviews.map(_.addToScore(-avgScore)))
      }
    }

    val normalizedReviews = normalizedMovies.coalesce(8, shuffle = true).flatMap(_.movieReviews)
    normalizedReviews
  }

  // Execute recommendation task
  def execute(task: RecommendationTask) = {
    val reviewsInputFile = task.inputFile
    
    val reviews: RDD[MovieReview] = MovieIO.getMovieReviews(reviewsInputFile)  // Get all reviews
    val normalizedReviews: RDD[MovieReview] = normalizeReviews(reviews).cache() // Normalize reviews score

    val reverseMovieIDs: RDD[(Long, String)] =
      normalizedReviews.map(r => (toID(r.movieId), r.movieId)).distinct(8).cache()

    val userMovies: RDD[(String, Iterable[MovieReview])] = // Group movie reviews by username
      normalizedReviews.groupBy(_.userId).cache()

    // Finished preparation

    val ratings: RDD[Rating[Long]] =
      normalizedReviews.map(r => Rating(toID(r.userId), toID(r.movieId), r.score.toFloat)).cache()

    // Train using ALS
    val model: LongMatrixFactorizationModel = als(ratings)


    // Get recommendations

    val taskUserIDs: Seq[(String, Long)] =
      task.users.view.map(userName => (userName, toID(userName)))

    val userVectors: Seq[(String, Array[Double])] = taskUserIDs.map { // Get user's ALS' feature array
      case (userName, userID) => (userName, model.userFeatures.lookup(userID).headOption match {
        case None => Array.fill(rank)(0.0)
        case Some(features) => features
      })
    }

    val userRecommendations: Seq[Recommendation] = userVectors.map {  // Recommend
      case (userName, userVector) =>
        val movies: Seq[(Long, Double)] =
          rankNewMovies(
            userVector,
            model,
            userMovies.lookup(userName).headOption    // The movies which the user has already seen
              .fold[Set[Long]](Set.empty)(i => i.map(r => toID(r.movieId)).toSet),
            numRecommendations)

        val movieRecommendation: Array[String] =
          movies.map {            // Movies recommendations
            case (movieID, rating) =>
              (reverseMovieIDs.lookup(movieID).head, // Get movie name from it's hash
                rating)
            // Every movie in the model should have come from the input.
          }.sortBy{
            case (movieName, rating) => (-rating, movieName) // sort by rating and than by movie name
          }.map(_._1)     // movie names to recommend
            .toArray
        // Recommendation
        Recommendation(userName, movieRecommendation)
    }

    // Output recommendations
    val fileOutput: PrintStream = new PrintStream(new FileOutputStream(task.outputFile))

    userRecommendations.foreach(fileOutput.print)

    fileOutput.close()
  }

  // Get movie ranks for user
  def rankNewMovies(userVector: Array[Double],            // User's feature array
                    model: LongMatrixFactorizationModel,  // ALS model
                    viewedMovies: Set[Long],              // Movies user already saw
                    numRecommendations: Int): Seq[(Long, Double)] = {
    LongMatrixFactorizationModel.allRecommendations(userVector, model.productFeatures)
      .filter { // Recommend only movies the user hasn't seen already
        case (movieID, score) => !viewedMovies.contains(movieID)
      }.take(numRecommendations).toSeq
  }

  // ------------------------------------------------------------------

  // SHA-256 hash
  def hash(text: String): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(text.getBytes("UTF-8"))
    md.digest()
  }

  // ALS algorithm works on Longs and not on strings - uniquely
  // map movieName/userName to a LONG value
  def toID(s: String): Long = {
    val bytes = hash(s)
    BigInt(bytes).toLong // take the lower 64 bits from the 256 bits.
    // Up to 2^32 unique ids.
  }

}
