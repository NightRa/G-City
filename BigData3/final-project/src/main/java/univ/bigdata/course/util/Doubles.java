package univ.bigdata.course.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class Doubles {
    public static double round(double d) {
        int places = 5;
        return new BigDecimal(d).setScale(places, RoundingMode.HALF_UP).doubleValue();
    }
}
