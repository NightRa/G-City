package univ.bigdata.course.util;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static univ.bigdata.course.util.Pair.pair;

public class Maps {
    public static <K, A, B> Map<K, B> mapValues(Map<K, A> map, Function<A, B> f) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> f.apply(e.getValue())));
    }

    public static <K, A, B> Map<K, B> mapValuesMaybe(Map<K, A> map, Function<A, Optional<B>> f) {
        return map.entrySet().stream().map(e -> pair(e.getKey(),f.apply(e.getValue()))).filter(p -> p.snd.isPresent()).map(p -> pair(p.fst, p.snd.get()))
                .collect(Collectors.toMap((Pair<K, B> p) -> p.fst, (Pair<K, B> p) -> p.snd));
    }
}
