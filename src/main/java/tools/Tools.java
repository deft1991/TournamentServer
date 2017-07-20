package tools;

/**
 * Created by PC on 13.07.2017.
 */
public class Tools {

    public static boolean isEmptyString(String data) {
        return data == null || data.trim().length() == 0;
    }

    public static Double round(Double data, int dividerLength) {
        int divider = 10 ^ dividerLength;
        double buf = data * divider;
        return (double) Math.round(buf) / divider;
    }
}
