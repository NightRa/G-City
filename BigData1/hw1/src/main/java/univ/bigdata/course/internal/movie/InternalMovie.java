package univ.bigdata.course.internal.movie;

import univ.bigdata.course.internal.util.Lazy;
import univ.bigdata.course.movie.Movie;

import java.util.Comparator;
import java.util.List;

import static java.util.Comparator.comparing;
import static univ.bigdata.course.internal.util.Doubles.round;

public final class InternalMovie {

    public final String movieId;

    public final Lazy<Double> avgScore;

    /**
     * The reviews for the given movie.
     * Invariant: all the reviews should have the @movieId field equal to the Movie's @movieId field.
     * Invariant: Must be non empty!
     **/
    public final List<InternalMovieReview> movieReviews;

    public InternalMovie(String movieId, List<InternalMovieReview> movieReviews) {
        if (movieReviews.size() == 0) throw new IllegalArgumentException("A Movie must have reviews.");
        this.movieId = movieId;
        this.movieReviews = movieReviews;
        this.avgScore = Lazy.lazy(this::computeAvgScore);
    }

    public double avgScore() {
        return avgScore.get();
    }

    public int numReviews() {
        return movieReviews.size();
    }

    public static Comparator<InternalMovie> movieIdOrder = comparing(movie -> movie.movieId);

    /*Comparator for sorting list by score*/
    public static Comparator<InternalMovie> movieScoreDescendingOrder = comparing(InternalMovie::avgScore).reversed();

    public static Comparator<InternalMovie> movieNumReviewsOrder = comparing(InternalMovie::numReviews);


    @SuppressWarnings("OptionalGetWithoutIsPresent")
    private double computeAvgScore() {
        // It's ok doing .get here, because of the invariant that the reviews list is non-empty.
        return movieReviews.stream().mapToDouble(review -> review.score).average().getAsDouble();
    }

    @Override
    public String toString() {
        return "Movie{" +
                "productId='" + movieId + '\'' +
                ", score=" + round(avgScore()) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        InternalMovie movie = (InternalMovie) o;

        if (!movieId.equals(movie.movieId)) return false;
        return movieReviews.equals(movie.movieReviews);
    }



}
