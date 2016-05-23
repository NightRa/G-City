package univ.bigdata.course;


import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

public class LinkedHashMapTest {
    @Test
    public void testSortingPreserve(){
        Map<Integer, Integer> map = new LinkedHashMap<>();
        map.put(0,0);
        map.put(1,1);
        map.put(2,2);

        Iterator<Map.Entry<Integer, Integer>> iterator = map.entrySet().iterator();
        Assert.assertTrue(iterator.next().getKey() == 0);
        Assert.assertTrue(iterator.next().getKey() == 1);
        Assert.assertTrue(iterator.next().getKey() == 2);
    }
}
