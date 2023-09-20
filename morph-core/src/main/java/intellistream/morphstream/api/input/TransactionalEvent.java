package intellistream.morphstream.api.input;

import intellistream.morphstream.engine.txn.TxnEvent;

import java.util.HashMap;
import java.util.List;

/**
 * This class is used in refined-API, to convert raw inputEvent to txnEvent
 * Differs from the original TxnEvent, this class only requires user to specify (keys, values, flags)
 * Currently, it inherits from TxnEvent to match the old TxnEvent logic (which involves partition information)
 * TODO: Abstract TxnEvent, push down partition-related info to its subclass. Refine the corresponding bolt logic as well.
 */

public class TransactionalEvent extends TxnEvent {
    private HashMap<String, List<String>> keyMap; //<TableName, keys> assume key must be string, including sourceKey, targetKey, and conditionKey
    private HashMap<String, Object> valueMap; //<valueName, value>
    private HashMap<String, String> valueTypeMap; //<valueName, valueDataType>
    private HashMap<String, String> conditionMap; //<conditionName, condition>
    private String flag; //"Deposit" or "Transfer"
    private Boolean isAbort = false;

    public TransactionalEvent(long bid,
                              HashMap<String, List<String>> keyMap,
                              HashMap<String, Object> valueMap,
                              HashMap<String, String> valueTypeMap,
                              HashMap<String, String> conditionMap,
                              String flag,
                              Boolean isAbort) {
        super(bid);
        this.keyMap = keyMap;
        this.valueMap = valueMap;
        this.valueTypeMap = valueTypeMap;
        this.conditionMap = conditionMap;
        this.flag = flag;
        this.isAbort = isAbort;
    }

    public TransactionalEvent(long bid) {
        super(bid);
    }

    public void setBid(long bid) {
        this.bid = bid;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Object getValue(String valueName) {
        return this.valueMap.get(valueName);
    }

    public String getKey(String tableName, int keyIndex) {
        return this.keyMap.get(tableName).get(keyIndex);
    }
    public Object getCondition(String conditionName) {
        return this.conditionMap.get(conditionName);
    }

    public HashMap<String, String> getValueTypeMap() {
        return this.valueTypeMap;
    }

    public String getFlag() {
        return this.flag;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (String tableName : keyMap.keySet()) {
            stringBuilder.append(tableName).append(":");
            for (String key : keyMap.get(tableName)) {
                stringBuilder.append(key).append(":");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() -1);
            stringBuilder.append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() -1);
        stringBuilder.append(";");
        for (String value : valueMap.keySet()) {
            stringBuilder.append(value).append(":").append(valueMap.get(value)).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() -1);
        stringBuilder.append(";");
        for (String valueType : valueTypeMap.keySet()) {
            stringBuilder.append(valueType).append(":").append(valueTypeMap.get(valueType)).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() -1);
        stringBuilder.append(";");
        stringBuilder.append(flag);
        stringBuilder.append(";");
        stringBuilder.append(isAbort);
        if (!conditionMap.isEmpty()) {
            stringBuilder.append(";");
            for (String condition : conditionMap.keySet()) {
                stringBuilder.append(condition).append(":").append(conditionMap.get(condition)).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() -1);
        }
        return stringBuilder.toString();
    }
}
