package application.tools;

import java.util.ArrayList;

/**
 * Created by tony on 6/5/2017.
 */
public class B_object extends object {
    private static final long serialVersionUID = 2225245845078346454L;
    //    byte[] value;
    byte value;

    public B_object() {
        value = 0;
        //new byte[1];//1 byte per object
        //r.nextBytes(value);
    }

    @Override
    public ArrayList<object> create_myObjectList(int size_state) {
        ArrayList<object> myOjbectList = new ArrayList<>();
        for (int i = 0; i < size_state; i++) {
            myOjbectList.add(new B_object());
        }
        return myOjbectList;

    }

    @Override
    public Object getValue() {
        return value;
    }
}
