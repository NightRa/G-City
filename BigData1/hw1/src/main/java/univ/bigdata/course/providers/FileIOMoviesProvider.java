package univ.bigdata.course.providers;

import univ.bigdata.course.internal.MoviesFunctions;
import univ.bigdata.course.internal.preprocessing.MovieIO;
import univ.bigdata.course.movie.MovieReview;

import java.io.IOException;
import java.util.Iterator;
import java.util.stream.Stream;

public class FileIOMoviesProvider implements MoviesProvider {

    private Iterator<MovieReview> iterator;

    public FileIOMoviesProvider(Iterator<MovieReview> reviewsIterator) {
        this.iterator = reviewsIterator;
    }

    public static Iterator<MovieReview> toIterator(MoviesProvider provider) {
        return new Iterator<MovieReview>() {
            @Override
            public boolean hasNext() {
                return provider.hasMovie();
            }

            @Override
            public MovieReview next() {
                return provider.getMovie();
            }
        };
    }

    @Override
    public boolean hasMovie() {
        return iterator.hasNext();
    }

    @Override
    public MovieReview getMovie() {
        return iterator.next();
    }
}
