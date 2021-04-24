package common.param;
import benchmark.TxnParam;
public class MicroParam extends TxnParam {
    int[] keys;
    //    List<DataBox>[] value_list;//Note, it should be arraylist instead of linkedlist as there's no add/remove later.
    public MicroParam(int numItems) {
        keys = new int[numItems];
//        value_list = new ArrayList[numItems];
    }
    public String keys(int i) {
        return String.valueOf(keys[i]);
    }
    public int[] keys() {
        return keys;
    }
    //    public List<DataBox>[] values() {
//        return value_list;
//    }
    public void set_keys(int access_id, int _keys) {
        keys[access_id] = _keys;
    }
}
