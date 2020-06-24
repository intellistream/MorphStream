package application.tools;


// (per socket):20M
// * 8 bits --> 0.1M long is significantly less than L3.

import java.util.ArrayList;

/**
 * Use char as the basic element.
 */
public class KB_object_AsString extends object {
    private static final long serialVersionUID = -397143864082731725L;
    char[] value;

    public KB_object_AsString() {
        value = new char[KB / 2];//each char occupy two bytes in java.
        for (int i = 0; i < KB / 2; i++) {
            value[i] = alphabet.charAt(r.nextInt(N));
        }
    }


    public ArrayList<object> create_myObjectList(int size_state) {
        ArrayList<object> myOjbectList = new ArrayList<>();
        for (int i = 0; i < size_state; i++) {
            myOjbectList.add(new KB_object_AsString());
        }
        return myOjbectList;
    }

    public String getValue() {
        return String.valueOf(value);
    }
}
