package univ.bigdata.course.part2

import java.io.{FileOutputStream, PrintStream}
import java.security.MessageDigest

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import univ.bigdata.course.part1.movie.MovieReview
import univ.bigdata.course.part1.preprocessing.MovieIO

case class Recommendation(userName: String, recommendations: Array[String]) {
  override def toString: String =
    s"Recommendations for $userName:" + recommendations.zipWithIndex.map {
      case (movieID, i) => s"${i + 1}. $movieID"
    }.mkString("\n", "\n", "\n======================================\n")
}

object Recommendation {


  val rank: Int = 10
  val iterations: Int = 500
  val numRecommendations: Int = 10

  def createRatings(reviews: RDD[MovieReview], movieIDs: RDD[(String, Long)], userIDs: RDD[(String, Long)]): RDD[Rating] = {
    val ratings =
      reviews.map(r => (r.movieId, r)).join(movieIDs).map {
        case (movieIDstr, (r, movieID)) => (r.userId, (r, movieID.toInt))
      }.join(userIDs).map {
        case (userIDstr, ((r, movieID), userID)) => Rating(userID.toInt, movieID, r.score)
      }

    ratings
  }

  def als(ratings: RDD[Rating]): MatrixFactorizationModel = {
    // blocks, blocks, rank, iterations, lambda, false, 1.0
    val model = new ALS().setRank(rank).setIterations(iterations)
      // .setLambda(1.0)
      .setIntermediateRDDStorageLevel(StorageLevel.MEMORY_ONLY).setFinalRDDStorageLevel(StorageLevel.MEMORY_ONLY)
      .run(ratings)
    model
  }

  def execute(task: RecommendationTask) = {
    val reviewsInputFile = task.inputFile
    /*val usersAmount = MovieIO.getMovieReviews(reviewsInputFile).map(r => (r.userId, 1)).reduceByKey(_+_).map(_._2)
    val (buckets, histogram): (Array[Double], Array[Long]) = usersAmount.histogram(105)
    println("buckets: " + buckets.mkString(", "))
    println("histogram: " + histogram.mkString(", "))*/

    val filteredReviews = MovieIO.getMovieReviews(reviewsInputFile).groupBy(_.userId).filter {
      case (userId, reviews) => reviews.size > 1
    }.flatMap(_._2)

    val normalizedMovies = {
      val rawMovies = MovieIO.batchMovieReviews(filteredReviews)
      rawMovies.map { movie =>
        val avgScore = movie.avgScore
        movie.copy(movieReviews = movie.movieReviews.map(_.addToScore(-avgScore)))
      }
    }.cache()

    val normalizedReviews = normalizedMovies.flatMap(_.movieReviews).cache()
    val userIDs: RDD[(String, Long)] = normalizedReviews.map(_.userId).distinct().sortBy(identity).zipWithIndex().cache()
    val movieIDs: RDD[(String, Long)] = normalizedReviews.map(_.movieId).distinct().sortBy(identity).zipWithIndex().cache()
    val reverseMovieIDs: RDD[(Long, String)] = movieIDs.map(_.swap).cache() // Check if the cache here degrades performance.

    // Finished preparation
    // Train using ALS

    val ratings: RDD[Rating] = createRatings(normalizedReviews, movieIDs, userIDs)
    val model: MatrixFactorizationModel = als(ratings)

    // Get recommendations

    val taskUserIDs: Seq[(String, Option[Long])] = task.users.view.map(userName => (userName, userIDs.lookup(userName).headOption))
    val userVectors: Seq[(String, Array[Double])] = taskUserIDs.map {
      case (userName, None) =>
        println(s"=====================================================================\n\n\n\n\nPROBLEM: User not found: $userName\n\n\n\n=====================================================================")
        (userName, Array.fill(rank)(0.0))
      case (userName, Some(userID)) => (userName, model.userFeatures.lookup(userID.toInt).head) // We know the user is in the input => the user is in the model.
    }
    val userRecommendations: Seq[Recommendation] = userVectors.map {
      case (userName, userVector) => Recommendation(userName, recommend(userVector, model.productFeatures, numRecommendations).map {
        case (movieID, rating) => reverseMovieIDs.lookup(movieID).head // Every movie in the model should have come from the input.
      })
    }

    // Output recommendations
    val fileOutput: PrintStream = new PrintStream(new FileOutputStream(task.outputFile))

    userRecommendations.foreach(fileOutput.print)

    fileOutput.close()
  }

  // ------------------------------------------------------------------


  def recommend(recommendToFeatures: Array[Double],
                recommendableFeatures: RDD[(Int, Array[Double])],
                num: Int): Array[(Int, Double)] = {

    val scored = recommendableFeatures.map { case (id, features) =>
      (id, blas.ddot(features.length, recommendToFeatures, 1, features, 1))
    }
    scored.top(num)(Ordering.by(_._2))
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
