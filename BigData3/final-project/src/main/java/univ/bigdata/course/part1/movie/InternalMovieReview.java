package univ.bigdata.course.part1.movie;

public class InternalMovieReview {

    public final String movieId;

    public final String userId;

    public final String profileName;

    public final Helpfulness helpfulness;

    public final double score;

    public final String timestamp;

    public final String summary;

    public final String review;

    public InternalMovieReview(String movieId, String userId, String profileName, Helpfulness helpfulness, double score, String timestamp, String summary, String review) {
        this.movieId = movieId;
        this.userId = userId;
        this.profileName = profileName;
        this.helpfulness = helpfulness;
        this.score = score;
        this.timestamp = timestamp;
        this.summary = summary;
        this.review = review;
    }

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
