package state_engine.utils;
import state_engine.storage.datatype.DataBox;

import java.util.LinkedList;
import java.util.List;
public class Utils {
    public static List<DataBox> memcpy(List<DataBox> data) {
        List<DataBox> copy = new LinkedList<>();
        for (DataBox item : data) {
            copy.add(item.clone());
        }
        return copy;
    }
}
