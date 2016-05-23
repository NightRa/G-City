package univ.bigdata.course;

import org.junit.Assert;
import org.junit.Test;
import univ.bigdata.course.internal.MoviesFunctions;
import univ.bigdata.course.part1.movie.Helpfulness;
import univ.bigdata.course.part1.movie.InternalMovie;
import univ.bigdata.course.part1.preprocessing.MovieIO;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static univ.bigdata.course.TestBuilders.*;

public class MoviesFunctionsTest {

    public final MoviesFunctions storage;

    public MoviesFunctionsTest() throws IOException, URISyntaxException {
        Path file = Paths.get(MoviesFunctions.class.getResource("/movies-sample.txt").toURI());
        this.storage = MovieIO.readMoviesFunctions(file);
    }


    @Test
    public void totalMoviesAvgTest1() {
        List<InternalMovie> movies = new ArrayList<>();
        // avg = 3
        movies.add(scoredMovie("movie1", 1, 2, 3, 4, 5));
        // avg = 4
        movies.add(scoredMovie("movie2", 5, 3, 5, 3));
        // avg = 5
        movies.add(scoredMovie("movie3", 5, 5, 5, 5));
        double totalAvg = new MoviesFunctions(movies).totalMoviesAverageScore();
        Assert.assertEquals(3.92308 /*expected*/, totalAvg /*actual*/, 1e-15 /*delta*/);
    }

    @Test
    public void testGroupMoviesById() {
        InternalMovie movie1 = namedMovie("movie1");
        InternalMovie movie2 = namedMovie("movie2");
        InternalMovie movie3 = namedMovie("movie3");
        Map<String, InternalMovie> moviesByName = MoviesFunctions.groupMoviesById(Arrays.asList(movie1, movie2, movie3));
        Map<String, InternalMovie> expected = new HashMap<>();
        expected.put("movie1", movie1);
        expected.put("movie2", movie2);
        expected.put("movie3", movie3);

        Assert.assertEquals(expected, moviesByName);
    }

    @Test
    public void testGroupMoviesByIdEmpty() {
        Map<String, InternalMovie> moviesByName = MoviesFunctions.groupMoviesById(Collections.emptyList());
        Map<String, InternalMovie> expected = new HashMap<>();
        Assert.assertEquals(expected, moviesByName);
    }

    @Test
    public void testMoviesCountNoList() {
        List<InternalMovie> movies = new ArrayList<>();
        MoviesFunctions storage = new MoviesFunctions(movies);
        Assert.assertEquals(0, storage.moviesCount());
    }

    @Test
    public void testMoviesCountFourMovies() {
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(namedMovie("movie1"));
        movies.add(namedMovie("movie2"));
        movies.add(namedMovie("movie3"));
        movies.add(namedMovie("movie4"));
        MoviesFunctions storage = new MoviesFunctions(movies);
        Assert.assertEquals(4, storage.moviesCount());
    }

    @Test
    public void testMostReviewedProduct17() {
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(movieManyReviews("movie1", 6));
        movies.add(movieManyReviews("movie2", 17));
        movies.add(movieManyReviews("movie3", 15));
        movies.add(movieManyReviews("movie4", 8));
        MoviesFunctions storage = new MoviesFunctions(movies);
        Assert.assertEquals("movie2", storage.mostReviewedProduct());
    }

    @Test
    public void testMostReviewedProduct56() {
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(movieManyReviews("movie1", 22));
        movies.add(movieManyReviews("movie2", 55));
        movies.add(movieManyReviews("movie3", 56));
        movies.add(movieManyReviews("movie4", 22));
        movies.add(movieManyReviews("movie5", 1));
        MoviesFunctions storage = new MoviesFunctions(movies);
        Assert.assertEquals("movie3", storage.mostReviewedProduct());
    }

    @Test
    public void testGetMoviesPercentile90() {
        List<InternalMovie> resultMovies = storage.getMoviesPercentile(90);
        InternalMovie expectedMovie1 = storage.lazyMoviesById.get().get("B00004CK40");
        List<InternalMovie> expectedList = new ArrayList<>();
        expectedList.add(expectedMovie1);
        Assert.assertEquals(expectedList, resultMovies);
    }

    @Test
    public void testMovieWithHighestAverage() {
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(scoredMovie("Tony", 10, 10, 10, 10, 5)); //avg = 9
        movies.add(scoredMovie("Yuval", 10, 10, 10, 10, 10, 10)); //avg = 6
        InternalMovie expected = scoredMovie("Ilan", 10, 10, 10, 10); // avg=10
        movies.add(expected);
        movies.add(scoredMovie("Charlie", 5, 5, 5)); // avg = 5
        MoviesFunctions storage = new MoviesFunctions(movies);
        Assert.assertEquals(expected, storage.movieWithHighestAverage());
    }

    @Test
    public void testMovieTopKMovieAverage() {
        List<InternalMovie> movies = new ArrayList<>();
        InternalMovie Godik = scoredMovie("Godik", 9, 2, 10, 5, 4); //avg 6
        InternalMovie Mubariky = scoredMovie("Mubariky", 4, 3, 10, 7); //avg 6
        movies.add(scoredMovie("Hankin", 8, 3, 1)); //avg 4
        movies.add(scoredMovie("Tannous", 5, 3)); //avg 4
        movies.add(scoredMovie("Alfassi", 1, 3, 9, 7)); //avg 5
        movies.add(Mubariky);
        movies.add(Godik);
        MoviesFunctions storage = new MoviesFunctions(movies);
        List<InternalMovie> expectedList = new ArrayList<>();
        expectedList.add(Godik);
        expectedList.add(Mubariky);
        Assert.assertEquals(expectedList, storage.getTopKMoviesAverage(2));
    }

    @Test
    public void testGetMoviesPercentile50() {
        List<InternalMovie> resultMovies = storage.getMoviesPercentile(50);
        InternalMovie expectedMovie1 = storage.lazyMoviesById.get().get("B00004CK40");
        InternalMovie expectedMovie2 = storage.lazyMoviesById.get().get("B0002IQNAG");
        List<InternalMovie> expectedList = new ArrayList<>();
        expectedList.add(expectedMovie1);
        expectedList.add(expectedMovie2);

        Assert.assertEquals(expectedList, resultMovies);

    }

    @Test
    public void testTopKHelpfullUsers1() {
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(TestBuilders.createMovieHelp("movie1", "user1", new Helpfulness(1,3), "user2", new Helpfulness(2, 3)));
        movies.add(TestBuilders.createMovieHelp("movie2", "user3", new Helpfulness(0,5), "user4", new Helpfulness(3, 9)));
        movies.add(TestBuilders.createMovieHelp("movie3", "user5", new Helpfulness(5,6), "user6", new Helpfulness(9, 10)));
        MoviesFunctions storage = new MoviesFunctions(movies);
        Map<String, Double> helpfuls = storage.topKHelpfullUsers(3);
        Assert.assertTrue(helpfuls.containsKey("user6"));
        Assert.assertTrue(helpfuls.containsKey("user5"));
        Assert.assertTrue(helpfuls.containsKey("user2"));
    }

    @Test
    public void testTopKHelpfullUsers2() {
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(TestBuilders.createMovieHelp("movie1", "user1", new Helpfulness(1,3), "user2", new Helpfulness(2, 3)));
        movies.add(TestBuilders.createMovieHelp("movie2", "user3", new Helpfulness(0,5), "user4", new Helpfulness(3, 9)));
        movies.add(TestBuilders.createMovieHelp("movie3", "user5", new Helpfulness(5,6), "user6", new Helpfulness(9, 10)));
        MoviesFunctions storage = new MoviesFunctions(movies);
        Map<String, Double> helpfuls = storage.topKHelpfullUsers(8);
        Assert.assertTrue(helpfuls.containsKey("user6"));
        Assert.assertTrue(helpfuls.containsKey("user5"));
        Assert.assertTrue(helpfuls.containsKey("user4"));
        Assert.assertTrue(helpfuls.containsKey("user2"));
        Assert.assertTrue(helpfuls.containsKey("user1"));
        Assert.assertTrue(helpfuls.containsKey("user5"));
        Assert.assertEquals(6, helpfuls.size());
    }

    @Test
    public void testTopKHelpfullUsers3() {
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(TestBuilders.createMovieHelp("movie1", "user1", new Helpfulness(1, 3), "user2", new Helpfulness(2, 3)));
        movies.add(TestBuilders.createMovieHelp("movie2", "user3", new Helpfulness(0, 5), "user4", new Helpfulness(3, 9)));
        movies.add(TestBuilders.createMovieHelp("movie3", "user5", new Helpfulness(5, 6), "user6", new Helpfulness(9, 10)));
        MoviesFunctions storage = new MoviesFunctions(movies);
        Map<String, Double> helpfuls = storage.topKHelpfullUsers(8);
        Assert.assertTrue(helpfuls.containsKey("user6"));
        Assert.assertTrue(helpfuls.containsKey("user5"));
        Assert.assertTrue(helpfuls.containsKey("user4"));
        Assert.assertTrue(helpfuls.containsKey("user3"));
        Assert.assertTrue(helpfuls.containsKey("user2"));
        Assert.assertTrue(helpfuls.containsKey("user1"));
        Assert.assertEquals(6, helpfuls.size());
    }

    public void testMovieTopKMovieAverage2() {
        List<InternalMovie> movies = new ArrayList<>();
        InternalMovie Godik = scoredMovie("Godik", 9, 2, 10, 5, 4); //avg 6
        InternalMovie Mubariky = scoredMovie("Mubariky", 4, 3, 10, 7); //avg 6
        movies.add(scoredMovie("Hankin", 8, 3, 1)); //avg 4
        movies.add(scoredMovie("Tannous", 5, 3)); //avg 4
        movies.add(scoredMovie("Alfassi", 1, 3, 9, 7)); //avg 5
        movies.add(Mubariky);
        movies.add(Godik);
        MoviesFunctions storage = new MoviesFunctions(movies);
        List<InternalMovie> expectedList = new ArrayList<>();
        expectedList.add(Godik);
        Assert.assertEquals(expectedList, storage.getTopKMoviesAverage(1));
    }

    @Test
    public void testTopKMoviesByNumReviews() {
        List<InternalMovie> movies = new ArrayList<>();
        InternalMovie Godik = scoredMovie("Godik", 9, 2, 10, 5, 4); //avg 6
        InternalMovie Mubariky = scoredMovie("Mubariky", 4, 3, 10, 7, 6); //avg 6
        movies.add(scoredMovie("Hankin", 8, 3, 1)); //avg 4
        movies.add(scoredMovie("Tannous", 5, 3)); //avg 4
        movies.add(scoredMovie("Alfassi", 1, 3, 9, 7)); //avg 5
        movies.add(Mubariky);
        movies.add(Godik);
        MoviesFunctions storage = new MoviesFunctions(movies);
        List<InternalMovie> expectedList = new ArrayList<>();
        expectedList.add(Godik);
        expectedList.add(Mubariky);
        Assert.assertEquals(expectedList, storage.getTopKMoviesAverage(2));
    }

    @Test
    public void testTopKMoviesByNumReviews2() {
        List<InternalMovie> movies = new ArrayList<>();
        InternalMovie Godik = scoredMovie("Godik", 9, 2, 10, 5, 4); //avg 6
        InternalMovie Mubariky = scoredMovie("Mubariky", 4, 3, 10, 7, 6); //avg 6
        movies.add(scoredMovie("Hankin", 8, 3, 1)); //avg 4
        movies.add(scoredMovie("Tannous", 5, 3)); //avg 4
        movies.add(scoredMovie("Alfassi", 1, 3, 9, 7)); //avg 5
        movies.add(Mubariky);
        movies.add(Godik);
        MoviesFunctions storage = new MoviesFunctions(movies);
        List<InternalMovie> expectedList = new ArrayList<>();
        expectedList.add(Godik);
        Assert.assertEquals(expectedList, storage.getTopKMoviesAverage(1));
    }

    @Test
    public void testMoviesReviewWordsCount(){
        List<InternalMovie> movies = new ArrayList<>();
        movies.add(TestBuilders.createMovieSummary("movie1", "Hello, how are you? my my my my my my. nice.. good good", "Ilan Godik is is is nice"));
        movies.add(TestBuilders.createMovieSummary("movie2", "Tony tannous and artem berger are learning OOP", "Germany has went out of the door in order to"));
        movies.add(TestBuilders.createMovieSummary("movie3", "When I first met becca, I was all what the hecka", "Susan boil's toothbrush"));
        MoviesFunctions storage = new MoviesFunctions(movies);
        Map<String, Long> wordsCount = storage.moviesReviewWordsCount(20);
        Iterator<Map.Entry<String, Long>> iterator = wordsCount.entrySet().iterator();
        Assert.assertEquals(20, wordsCount.keySet().size());
        Assert.assertEquals("my", iterator.next().getKey());
        Assert.assertEquals((Long) 3L, iterator.next().getValue());
    }
}
