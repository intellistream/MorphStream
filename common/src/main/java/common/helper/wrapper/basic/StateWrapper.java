package common.helper.wrapper.basic;
//import applications.state_engine.utils.Configuration;
import common.helper.Event;
import common.tools.B_object;
import common.tools.object;
import common.collections.Configuration;

import java.io.Serializable;
import java.util.ArrayList;
public abstract class StateWrapper<T> implements Serializable {
    private static final long serialVersionUID = 3276941944488848300L;
    protected Configuration config;
    /**
     * TODO: make it generic.
     */
    private String tuple_states = null;
    private String flag;
    public StateWrapper(boolean verbose, int size) {
        this(size);
        flag = "|" + Event.split_expression;
    }
    public StateWrapper(int size) {
        flag = null;
        create_states(size);
    }
    public String getFlags(int index_e) {
        if (index_e % 500 == 0)//we monitor per 100 event to reduce overhead.
            return flag;
        else return Event.null_expression;
    }
    private void create_states(int size) {
        if (size == 0) {
            return;
        }
        ArrayList<object> list;
//        if (size > 1000) {
//            list = new KB_object_AsString() // or KB_object_AsString
//                    .create_myObjectList(size / 1000);
//        } else {
        list = new B_object() // or KB_object_AsString
                .create_myObjectList(size);
//        }
        StringBuilder sb = new StringBuilder();
        for (object obj : list) {
            sb.append(obj.getValue());
        }
        tuple_states = sb.toString();//wrap all the states into one string.
    }
    /**
     * @return a new copy of states.
     */
    public String getTuple_states() {
        return tuple_states;
    }
}