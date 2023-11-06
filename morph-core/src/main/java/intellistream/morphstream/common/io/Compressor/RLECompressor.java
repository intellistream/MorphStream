package intellistream.morphstream.common.io.Compressor;

import java.text.NumberFormat;
import java.text.ParseException;

public class RLECompressor implements Compressor {
    public static String encode(String in) {
        String outString = "";
        char c = in.charAt(0);
        int count = 0;

        for (char x : in.toCharArray()) {
            if (x == c) {
                count++;
            } else {
                outString = outString + count + c;
                c = x;
                count = 1;
            }
        }
        outString = outString + count + c;
        return outString;
    }

    public static String decode(String in) {
        String outString = "";
        int i = 0, step = 0;

        while (in.length() > 0) {
            try {
                i = NumberFormat.getInstance().parse(in).intValue();
            } catch (ParseException e) {
                e.printStackTrace();
            }

            step = ((String.valueOf(i)).length());
            char c = in.charAt(step);

            for (int x = 0; x < i; x++) {
                outString += c;
            }
            in = in.substring(step + 1);
        }
        return outString;
    }

    @Override
    public String compress(String in) {
        return encode(in);
    }

    @Override
    public String uncompress(String in) {
        return decode(in);
    }
}
