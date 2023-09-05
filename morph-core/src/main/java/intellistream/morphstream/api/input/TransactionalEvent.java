package intellistream.morphstream.api.input;

import intellistream.morphstream.engine.txn.TxnEvent;

import java.util.HashMap;

/**
 * This class is used in refined-API, to convert raw inputEvent to txnEvent
 * Differs from the original TxnEvent, this class only requires user to specify (keys, values, flags)
 * Currently, it inherits from TxnEvent to match the old TxnEvent logic (which involves partition information)
 * TODO: Abstract TxnEvent, push down partition-related info to its subclass. Refine the corresponding bolt logic as well.
 */

public class TransactionalEvent extends TxnEvent {
    private HashMap<String, String> keyMap; //<keyName, key> assume key must be string, including sourceKey, targetKey, and conditionKey
    private HashMap<String, Object> valueMap; //<valueName, value>
    private HashMap<String, String> valueTypeMap; //<valueName, valueDataType>
    private String flag; //"Deposit" or "Transfer"
    private Boolean isAbort = false;

    public TransactionalEvent(long bid,
                              HashMap<String, String> keyMap,
                              HashMap<String, Object> valueMap,
                              HashMap<String, String> valueTypeMap,
                              String flag,
                              Boolean isAbort) {
        super(bid);
        this.keyMap = keyMap;
        this.valueMap = valueMap;
        this.valueTypeMap = valueTypeMap;
        this.flag = flag;
        this.isAbort = isAbort;
    }

    public TransactionalEvent(long bid) {
        super(bid);
    }

    public void setKeyMap(HashMap<String, String> keyMap) {
        this.keyMap = keyMap;
    }

    public void setValueMap(HashMap<String, Object> valueMap) {
        this.valueMap = valueMap;
    }

    public void setValueTypeMap(HashMap<String, String> valueTypeMap) {
        this.valueTypeMap = valueTypeMap;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public HashMap<String, String> getKeyMap() {
        return this.keyMap;
    }

    public HashMap<String, Object> getValueMap() {
        return this.valueMap;
    }

    public HashMap<String, String> getValueTypeMap() {
        return this.valueTypeMap;
    }

    public String getFlags() {
        return this.flag;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (String key : keyMap.keySet()) {
            stringBuilder.append(key).append(":").append(keyMap.get(key)).append(",");
        }
        stringBuilder.append(";");
        for (String value : valueMap.keySet()) {
            stringBuilder.append(value).append(":").append(valueMap.get(value)).append(",");
        }
        stringBuilder.append(";");
        for (String valueType : valueTypeMap.keySet()) {
            stringBuilder.append(valueType).append(":").append(valueTypeMap.get(valueType)).append(",");
        }
        stringBuilder.append(";");
        stringBuilder.append(flag);
        stringBuilder.append(";");
        stringBuilder.append(isAbort);
        return stringBuilder.toString();
    }
}