package univ.bigdata.course;

import org.junit.Assert;
import org.junit.Test;
import univ.bigdata.course.internal.MoviesFunctions;
import univ.bigdata.course.part1.movie.InternalMovie;

import java.util.ArrayList;
import java.util.List;

import static univ.bigdata.course.TestBuilders.*;

public class MovieTest {

    @Test
    public void testMovieAvg() {
        InternalMovie movie1 = scoredMovie("movie1", 1, 5, 3, 4, 1, 1);
        double expectedAvg = (double) (1 + 5 + 3 + 4 + 1 + 1) / 6;
        Assert.assertEquals(expectedAvg, movie1.avgScore(), 1e-15);
    }

    @Test
    public void testMovieAvgOneReview() {
        InternalMovie movieNoReviews = scoredMovie("movie2", 3);
        double expectedAvg = 3;
        Assert.assertEquals(expectedAvg, movieNoReviews.avgScore(), 1e-15);
    }

    @Test
    public void testMostPopularMovieReviewedByKUsers () {
        InternalMovie movie1 = scoredMovie("Yuval", 1, 5, 3, 3, 5, 9, 10);
        InternalMovie movie2 = scoredMovie("Tony", 0, 0, 4, 3, 1);
        List<InternalMovie> vika = new ArrayList<>(2);
        vika.add(movie1);
        vika.add(movie2);
        MoviesFunctions storage = new MoviesFunctions(vika);
        String id = storage.mostPopularMovieReviewedByKUsers(6);
        Assert.assertEquals(id, movie1.movieId);
    }

    @Test
    public void testMoviePrecentile()
    {
        List<InternalMovie> movies = new ArrayList<>();
        InternalMovie movie1 = scoredMovie("AA", 5, 7, 9, 8, 9, 9, 9);
        InternalMovie movie2 = scoredMovie("BB", 8);
        InternalMovie movie3 = scoredMovie("cc", 10);
        InternalMovie movie4 = scoredMovie("dd", 10);
        movies.add(movie1);
        movies.add(movie2);
        movies.add(movie3);
        movies.add(movie4);
        MoviesFunctions storage = new MoviesFunctions(movies);
        List<InternalMovie> t = storage.getMoviesPercentile(75);
        Assert.assertEquals(movie3.movieId, t.get(0).movieId);
    }

}
