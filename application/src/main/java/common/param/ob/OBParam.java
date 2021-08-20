package common.param.ob;

import content.common.TxnParam;

public class OBParam extends TxnParam {
    int[] itemID;

    public OBParam(int numItems) {
        itemID = new int[numItems];
    }

    public String keys(int i) {
        return String.valueOf(itemID[i]);
    }

    public int[] keys() {
        return itemID;
    }

    public void set_keys(int access_id, int _keys) {
        itemID[access_id] = _keys;
    }
}
