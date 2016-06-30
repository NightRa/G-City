package univ.bigdata.course.part1.movie;

import java.io.Serializable;

public class MovieReview implements Serializable {

    public final String movieId;

    public final String userId;

    public final String profileName;

    public final Helpfulness helpfulness;

    public final double score;

    public final Long timestamp;

    public final String summary;

    public final String review;
	// add to score
    public MovieReview addToScore(double delta) {
        return new MovieReview(movieId, userId, profileName, helpfulness, score + delta, timestamp, summary, review);
    }

    public MovieReview(String movieId, String userId, String profileName, Helpfulness helpfulness, double score, Long timestamp, String summary, String review) {
        this.movieId = movieId;
        this.userId = userId;
        this.profileName = profileName;
        this.helpfulness = helpfulness;
        this.score = score;
        this.timestamp = timestamp;
        this.summary = summary;
        this.review = review;
    }
	// Check if two MovieReview objects are equal
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MovieReview that = (MovieReview) o;

        if (Double.compare(that.score, score) != 0) return false;
        if (!movieId.equals(that.movieId)) return false;
        if (!userId.equals(that.userId)) return false;
        if (!profileName.equals(that.profileName)) return false;
        if (!helpfulness.equals(that.helpfulness)) return false;
        if (!timestamp.equals(that.timestamp)) return false;
        if (!summary.equals(that.summary)) return false;
        return review.equals(that.review);
    }
	// Hash code
    @Override
    public int hashCode() {
        int result;
        long temp;
        result = movieId.hashCode();
        result = 31 * result + userId.hashCode();
        result = 31 * result + profileName.hashCode();
        result = 31 * result + helpfulness.hashCode();
        temp = Double.doubleToLongBits(score);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + timestamp.hashCode();
        result = 31 * result + summary.hashCode();
        result = 31 * result + review.hashCode();
        return result;
    }
	// Movie review output
    @Override
    public String toString() {
        return "MovieReview{" +
                "movieId=" + movieId +
                ", userId='" + userId + '\'' +
                ", profileName='" + profileName + '\'' +
                ", helpfulness='" + helpfulness.show() + '\'' +
                ", score=" + score +
                ", timestamp=" + timestamp +
                ", summary='" + summary + '\'' +
                ", review='" + review + '\'' +
                '}';
    }
}
