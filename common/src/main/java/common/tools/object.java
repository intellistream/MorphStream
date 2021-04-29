package common.tools;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;
/**
 * Created by tony on 6/5/2017.
 */
public abstract class object implements Serializable {
    private static final long serialVersionUID = 5884774060475035093L;
    final int MB = 1024 * 1024;
    final int KB = 1024;
    final String alphabet = "0123456789ABCDE";
    final int N = alphabet.length();
    Random r = new Random();
    public abstract ArrayList<object> create_myObjectList(int size_state);
    public abstract Object getValue();
}
