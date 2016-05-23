package univ.bigdata.course.part1.preprocessing;

import univ.bigdata.course.part1.MoviesFunctions;
import univ.bigdata.course.part1.movie.Movie;
import univ.bigdata.course.part1.movie.InternalMovieReview;
import univ.bigdata.course.part1.preprocessing.internal.MovieIOInternals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MovieIO {
    /**
     * May assume that the file exists.
     **/
    public static List<InternalMovieReview> getMovieReviews(Path inputFilePath) throws IOException {
        Stream<String> reviewsLines = Files.readAllLines(inputFilePath).stream();
        Stream<InternalMovieReview> movieReviews = reviewsLines.map(MovieIOInternals::lineToReview);
        return movieReviews.collect(Collectors.toList());
    }

    public static MoviesFunctions readMoviesFunctions(Path inputFilePath) throws IOException {
        List<InternalMovieReview> movieReviews = MovieIO.getMovieReviews(inputFilePath);
        List<Movie> movies = batchMovieReviews(movieReviews);
        return new MoviesFunctions(movies);
    }

    public static List<Movie> batchMovieReviews(List<InternalMovieReview> reviews) {

        Map<String, List<InternalMovieReview>> reviewsGrouped =
                reviews
                        .stream()
                        .collect(Collectors.groupingBy(r -> r.movieId));
        return reviewsGrouped
                .keySet()
                .stream()
                .map(id -> new Movie(id, reviewsGrouped.get(id)))
                .collect(Collectors.toList());
    }
}
