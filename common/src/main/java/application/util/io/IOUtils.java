package application.util.io;

import java.io.InputStream;
import java.util.Scanner;

public class IOUtils {
    public static String convertStreamToString(InputStream is) {
        Scanner s = null;
        try {
            s = new Scanner(is).useDelimiter("\\A");
        } catch (Exception ex) {
            System.out.println("FUCKING error!" + ex.getMessage());
        }
        return s.hasNext() ? s.next() : "";
    }

    public static String convertStreamToString(InputStream is, String charsetName) {
        Scanner s = new Scanner(is, charsetName).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
