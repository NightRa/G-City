package univ.bigdata.course.util;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Streams {
    public static <A, M> M mapReduce(Stream<A> data, Function<A, M> f, M unit, BinaryOperator<M> combine) {
        return data.map(f).reduce(unit, combine);
    }

    public static <A> Stream<A> fromIterator(Iterator<A> iterator) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED),
                false);
    }
}
