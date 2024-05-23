package intellistream.morphstream.api.input;

import intellistream.morphstream.engine.txn.TxnEvent;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class is used in refined-API, to convert raw inputEvent to txnEvent
 * Differs from the original TxnEvent, this class only requires user to specify (keys, values, flags)
 * Currently, it inherits from TxnEvent to match the old TxnEvent logic (which involves partition information)
 */

public class TransactionalEvent extends TxnEvent {
    @Getter
    private HashMap<String, List<String>> keyMap; //<TableName, keys> assume key must be string, including sourceKey, targetKey, and conditionKey
    private HashMap<String, Object> paraMap; //<valueName, value>
    @Getter
    private HashMap<String, String> paraTypeMap; //<valueName, valueDataType>
    @Getter
    @Setter
    private String flag; //E.g., "Deposit" or "Transfer"
    private boolean isAbort = false;

    public TransactionalEvent(long bid,
                              HashMap<String, List<String>> keyMap,
                              HashMap<String, Object> paraMap,
                              HashMap<String, String> paraTypeMap,
                              String flag,
                              boolean isAbort) {
        super(bid);
        this.keyMap = keyMap;
        this.paraMap = paraMap;
        this.paraTypeMap = paraTypeMap;
        this.flag = flag;
        this.isAbort = isAbort;
    }

    public TransactionalEvent(long bid) {
        super(bid);
    }

    public void setBid(long bid) {
        this.bid = bid;
    }

    public Object getPara(String paraName) {
        return this.paraMap.get(paraName);
    }

    public String getKey(String tableName, int keyIndex) {
        return this.keyMap.get(tableName).get(keyIndex);
    }
    public List<String> getAllKeys(){
        List<String> keys = new ArrayList<>();
        for (String tableName : keyMap.keySet()) {
            keys.addAll(keyMap.get(tableName));
        }
        return keys;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(bid);
        stringBuilder.append(";");
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
        for (String value : paraMap.keySet()) {
            stringBuilder.append(value).append(":").append(paraMap.get(value)).append(",");
        }
        if (!paraMap.isEmpty())
            stringBuilder.deleteCharAt(stringBuilder.length() -1);
        stringBuilder.append(";");
        for (String valueType : paraTypeMap.keySet()) {
            stringBuilder.append(valueType).append(":").append(paraTypeMap.get(valueType)).append(",");
        }
        if (!paraTypeMap.isEmpty())
            stringBuilder.deleteCharAt(stringBuilder.length() -1);
        stringBuilder.append(";");
        stringBuilder.append(flag);
        stringBuilder.append(";");
        stringBuilder.append(isAbort);
        return stringBuilder.toString();
    }
}
