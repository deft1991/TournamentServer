package tools;

/**
 * Created by PC on 13.07.2017.
 */
public class Tools {

    public static boolean isEmptyString(String data) {
        return data == null || data.trim().length() == 0;
    }

    public static Float round(Float data, int dividerLength) {
        int divider = 10 ^ dividerLength;
        float buf = data * divider;
        return (float) Math.round(buf) / divider;
    }
}
