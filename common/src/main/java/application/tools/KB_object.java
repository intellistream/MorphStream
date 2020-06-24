package application.tools;


// (per socket):20M
// * 8 bits --> 0.1M long is significantly less than L3.

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

/**
 * each myObject is 8 bytes
 * let's change it to 1 MB!! that is 1*1024*1024 bytes. This is too slow..
 * let's change it to 1 KB that is 1*1024 bytes.
 */
public class KB_object implements Serializable {
	private static final long serialVersionUID = -9073964378669329849L;
	final int MB = 1024 * 1024;
    final int KB = 1024;

    byte[] value;

    /*public myObject(byte[] value) {
        this.value = value;
    }*/
    Random r = new Random();

    public KB_object() {
        value = new byte[KB];
        r.nextBytes(value);
    }

    public static ArrayList<KB_object> create_myObjectList(int size_state) {
        ArrayList<KB_object> myOjbectList = new ArrayList<>();
        for (int i = 0; i < size_state; i++) {
            myOjbectList.add(new KB_object());
        }
        return myOjbectList;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
