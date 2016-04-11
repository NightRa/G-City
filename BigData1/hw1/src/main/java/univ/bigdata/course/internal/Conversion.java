package univ.bigdata.course.internal;

import univ.bigdata.course.internal.movie.InternalMovie;
import univ.bigdata.course.internal.movie.InternalMovieReview;
import univ.bigdata.course.internal.preprocessing.MovieIO;
import univ.bigdata.course.internal.preprocessing.internal.MovieIOInternals;
import univ.bigdata.course.internal.util.Streams;
import univ.bigdata.course.movie.Movie;
import univ.bigdata.course.movie.MovieReview;
import univ.bigdata.course.providers.FileIOMoviesProvider;
import univ.bigdata.course.providers.MoviesProvider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static univ.bigdata.course.internal.util.Doubles.round;

public class Conversion {
    public static MoviesFunctions readFromFile(Path path) throws IOException {
        return MovieIO.readMoviesFunctions(path);
    }

    public static MovieReview toNativeReview(InternalMovieReview review) {
        return new univ.bigdata.course.movie.MovieReview(
                new Movie(review.movieId, review.score), review.userId,
                review.profileName, review.helpfulness.toString(),
                new Date(1000 * Long.valueOf(review.timestamp)), review.summary, review.review);
    }

    public static MoviesFunctions fromProvider(MoviesProvider provider) {
        Stream<MovieReview> reviews =
                Streams.fromIterator(FileIOMoviesProvider.toIterator(provider));

        List<InternalMovieReview> internalReviews = reviews.map(Conversion::toInternalReview)
                .collect(Collectors.toList());

        List<InternalMovie> movies = MovieIO.batchMovieReviews(internalReviews);
        return new MoviesFunctions(movies);
    }

    public static MoviesProvider funcsToProvider(MoviesFunctions func) {
        Stream<MovieReview> reviews = func.movies.stream()
                .flatMap(movie -> movie.movieReviews.stream()
                        .map(Conversion::toNativeReview));
        return new FileIOMoviesProvider(reviews.iterator());
    }

    public static MoviesProvider readMoviesProvider(Path inputFilePath) throws IOException {
        return funcsToProvider(readFromFile(inputFilePath));
    }

    public static InternalMovieReview toInternalReview(MovieReview review) {
        return new InternalMovieReview(
                review.getMovie().getProductId(),
                review.getUserId(),
                review.getProfileName(),
                MovieIOInternals.parseHelpfulness(review.getHelpfulness()),
                review.getMovie().getScore(),
                review.getTimestamp().toString(),
                review.getSummary(),
                review.getReview());
    }


    public static Movie toNativeMovie(InternalMovie movie) {
        return new Movie(movie.movieId,
                round(movie.avgScore()));
    }
}
