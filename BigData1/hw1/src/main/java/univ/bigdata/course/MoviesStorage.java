package univ.bigdata.course;

import univ.bigdata.course.internal.Conversion;
import univ.bigdata.course.internal.MoviesFunctions;
import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.providers.MoviesProvider;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static univ.bigdata.course.internal.Conversion.toNativeMovie;

/**
 * Main class which capable to keep all information regarding movies review.
 * Has to implements all methods from @{@link IMoviesStorage} interface.
 * Also presents functionality to answer different user queries, such as:
 * <p>
 * 1. Total number of distinct movies reviewed.
 * 2. Total number of distinct users that produces the review.
 * 3. Average review score for all movies.
 * 4. Average review score per single movie.
 * 5. Most popular movie reviewed by at least "K" unique users
 * 6. Word count for movie review, select top "K" words
 * 7. K most helpful users
 */
public class MoviesStorage implements IMoviesStorage {

    public final MoviesFunctions funcs;

    public MoviesStorage(final MoviesProvider provider) {
        this.funcs = Conversion.fromProvider(provider);
    }

    @Override
    public double totalMoviesAverageScore() {
        return funcs.totalMoviesAverageScore();
    }

    @Override
    public double totalMovieAverage(String productId) {
        return funcs.totalMovieAverage(productId);
    }

    @Override
    public List<Movie> getTopKMoviesAverage(long topK) {
        return funcs.getTopKMoviesAverage(topK).stream()
                .map(Conversion::toNativeMovie)
                .collect(Collectors.toList());
    }

    @Override
    public Movie movieWithHighestAverage() {
        return toNativeMovie(funcs.movieWithHighestAverage());
    }

    @Override
    public List<Movie> getMoviesPercentile(double percentile) {
        return funcs.getMoviesPercentile(percentile).stream()
                .map(Conversion::toNativeMovie)
                .collect(Collectors.toList());
    }

    @Override
    public String mostReviewedProduct() {
        return funcs.mostReviewedProduct();
    }

    @Override
    public Map<String, Long> reviewCountPerMovieTopKMovies(int topK) {
        return funcs.reviewCountPerMovieTopKMovies(topK);
    }

    @Override
    public String mostPopularMovieReviewedByKUsers(int numOfUsers) {
        return funcs.mostPopularMovieReviewedByKUsers(numOfUsers);
    }

    @Override
    public Map<String, Long> moviesReviewWordsCount(int topK) {
        return funcs.moviesReviewWordsCount(topK);
    }

    @Override
    public Map<String, Long> topYMoviewsReviewTopXWordsCount(int topMovies, int topWords) {
        return funcs.topYMoviewsReviewTopXWordsCount(topMovies, topWords);
    }

    @Override
    public Map<String, Double> topKHelpfullUsers(int k) {
        return funcs.topKHelpfullUsers(k);
    }

    @Override
    public long moviesCount() {
        return funcs.moviesCount();
    }
}
