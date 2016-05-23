package univ.bigdata.course.part1.movie;

import univ.bigdata.course.util.Lazy;

import java.util.Comparator;
import java.util.List;

import static java.util.Comparator.comparing;
import static univ.bigdata.course.util.Doubles.round;

public final class Movie {

    public final String movieId;

    public final Lazy<Double> avgScore;

    /**
     * The reviews for the given movie.
     * Invariant: all the reviews should have the @movieId field equal to the Movie's @movieId field.
     * Invariant: Must be non empty!
     **/
    public final List<InternalMovieReview> movieReviews;

    public Movie(String movieId, List<InternalMovieReview> movieReviews) {
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

    public static Comparator<Movie> movieIdOrder = comparing(movie -> movie.movieId);

    /*Comparator for sorting list by score*/
    public static Comparator<Movie> movieScoreDescendingOrder = comparing(Movie::avgScore).reversed();

    public static Comparator<Movie> movieNumReviewsOrder = comparing(Movie::numReviews);


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

        Movie movie = (Movie) o;

        if (!movieId.equals(movie.movieId)) return false;
        return movieReviews.equals(movie.movieReviews);
    }



}
