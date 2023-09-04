package intellistream.morphstream.api.input;

import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;

import javax.xml.validation.Schema;
import java.util.HashMap;

/**
 * This class is used in refined-API, to convert raw inputEvent to txnEvent
 * Differs from the original TxnEvent, this class only requires user to specify (keys, values, flags)
 * Currently, it inherits from TxnEvent to match the old TxnEvent logic (which involves partition information)
 * TODO: Abstract TxnEvent, push down partition-related info to its subclass. Refine the corresponding bolt logic as well.
 */

public class TransactionalEvent extends TxnEvent {
    private HashMap<String, String> keyMap; //<keyName, key> assume key must be string, including sourceKey, targetKey, and conditionKey
    private HashMap<String, String> valueMap; //<valueName, value>
    private HashMap<String, String> valueTypeMap; //<valueName, valueDataType>
    private String flag; //"Deposit" or "Transfer"
    private HashMap<String, SchemaRecord> record_refs;

    public TransactionalEvent(long bid,
                              HashMap<String, String> keyMap,
                              HashMap<String, String> valueMap,
                              HashMap<String, String> valueTypeMap,
                              String flag) {
        super(bid);
        this.keyMap = keyMap;
        this.valueMap = valueMap;
        this.valueTypeMap = valueTypeMap;
        this.flag = flag;
    }

    public TransactionalEvent(long bid) {
        super(bid);
    }

    public void setKeyMap(HashMap<String, String> keyMap) {
        this.keyMap = keyMap;
    }

    public void setValueMap(HashMap<String, String> valueMap) {
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

    public HashMap<String, String> getValueMap() {
        return this.valueMap;
    }

    public HashMap<String, String> getValueTypeMap() {
        return this.valueTypeMap;
    }

    public String getFlags() {
        return this.flag;
    }
}
