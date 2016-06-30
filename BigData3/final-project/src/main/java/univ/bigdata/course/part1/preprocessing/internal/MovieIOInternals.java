package univ.bigdata.course.part1.preprocessing.internal;

import univ.bigdata.course.part1.movie.Helpfulness;
import univ.bigdata.course.part1.movie.MovieReview;

import java.util.function.Supplier;
import java.util.stream.Stream;

public class MovieIOInternals {
	// Try to get the value,in case of failure throws error String
    public static <A> A tryOrError(Supplier<A> value, String error) {
        try {
            return value.get();
        } catch (Exception e) {
            throw new IllegalArgumentException(error);
        }
    }

    public static MovieReview lineToReview(String line) throws IllegalArgumentException {
        // We did not check the correctness of names of the fields
        String[] fields = Stream.of(line.split("\t")).map(MovieIOInternals::infoComponent).toArray(String[]::new);

        if (fields.length != 8) {
            throw new IllegalArgumentException("Invalid line in the input file: \"" + line + "\".");
        }

        String prodId = fields[0];
        String userId = fields[1];
        String profileName = fields[2];
        String helpfulnessStr = fields[3];
        String scoreStr = fields[4];
        String timeStr = fields[5];
        String summary = fields[6];
        String text = fields[7];

        Helpfulness helpfulness = parseHelpfulness(helpfulnessStr);
        double score = tryOrError(() -> Double.parseDouble(scoreStr), "Invalid score: \"" + scoreStr + "\"");
        long time = tryOrError(() -> Long.parseLong(timeStr), "Invalid time: \"" + timeStr + "\"");
        return new MovieReview(prodId, userId, profileName, helpfulness, score, time, summary, text);
    }

    public static String infoComponent(String field) throws IllegalArgumentException {
        int i = field.indexOf(": ");
        if(i == -1) throw new IllegalArgumentException("Invalid input field format: \"" + field + "\"");
        return field.substring(i + 2);
    }
	// Parse to Helpfulness
    public static Helpfulness parseHelpfulness(String helpfulnessStr) throws IllegalArgumentException {
        String[] split = helpfulnessStr.split("/");
        if(split.length != 2) {
            throw new IllegalArgumentException("Illegal helpfulness string: \"" + helpfulnessStr + "\"");
        }
        int positive = tryOrError(() -> Integer.parseUnsignedInt(split[0]), "Invalid helpfulness: \"" + helpfulnessStr + "\"");
        int total = tryOrError(() -> Integer.parseUnsignedInt(split[1]), "Invalid helpfulness: \"" + helpfulnessStr + "\"");
        return new Helpfulness(positive, total);
    }


}
