package univ.bigdata.course.part2

import java.io.{FileOutputStream, PrintStream}
import java.security.MessageDigest

import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.{LongALS, LongMatrixFactorizationModel, Ranking}
import org.apache.spark.rdd.RDD
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO
import univ.bigdata.course.part2.Recommendation._

case class Recommendation(userName: String, recommendations: Array[String]) {
  override def toString: String =
    s"Recommendations for $userName:" + recommendations.zipWithIndex.map {
      case (movieID, i) => s"${i + 1}. $movieID"
    }.mkString("\n", "\n", "\n======================================\n")
}

object Recommendation {
  val rank: Int = 150
  val iterations: Int = 15
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

  def execute(task: RecommendationTask) = {
    val reviewsInputFile = task.inputFile

    val reviews = MovieIO.getMovieReviews(reviewsInputFile)
    val normalizedReviews = normalizeReviews(reviews).cache()

    val reverseMovieIDs: RDD[(Long, String)] = normalizedReviews.map(r => (toID(r.movieId), r.movieId)).distinct(8).cache()

    // Finished preparation

    val ratings: RDD[Rating[Long]] = normalizedReviews.map(r => Rating(toID(r.userId), toID(r.movieId), r.score.toFloat)).cache()
    // Train using ALS
    val model: LongMatrixFactorizationModel = als(ratings)


    // Get recommendations

    val taskUserIDs: Seq[(String, Long)] = task.users.view.map(userName => (userName, toID(userName)))
    val userVectors: Seq[(String, Array[Double])] = taskUserIDs.map {
      case (userName, userID) => (userName, model.userFeatures.lookup(userID).headOption match {
        case None => Array.fill(rank)(0.0)
        case Some(features) => features
      })
    }

    val userRecommendations: Seq[Recommendation] = userVectors.map {
      case (userName, userVector) => Recommendation(userName, LongMatrixFactorizationModel.recommend(userVector, model.productFeatures, numRecommendations).map {
        case (movieID, rating) => reverseMovieIDs.lookup(movieID).head // Every movie in the model should have come from the input.
      })
    }

    // Output recommendations
    val fileOutput: PrintStream = new PrintStream(new FileOutputStream(task.outputFile))

    userRecommendations.foreach(fileOutput.print)

    fileOutput.close()
  }

  // ------------------------------------------------------------------

  def hash(text: String): Array[Byte] = {
    val md = MessageDigest.getInstance("SHA-256")
    md.update(text.getBytes("UTF-8"))
    md.digest()
  }

  def toID(s: String): Long = {
    val bytes = hash(s)
    BigInt(bytes).toLong // take the lower 64 bits from the 256 bits.
    // Up to 2^32 unique ids.
  }

}
