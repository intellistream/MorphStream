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

    private final HashMap<String, String> keys; //<keyName, key> assume key must be string
    private final HashMap<String, String> values; //<valueName, value>
    private final HashMap<String, String> valueTypes; //<valueName, valueDataType>
    private final String[] flags; //["Deposit", "Transfer"]

    public TransactionalEvent(long bid,
                              HashMap<String, String> keys,
                              HashMap<String, String> values,
                              HashMap<String, String> valueTypes,
                              String[] flags) {
        super(bid);
        this.keys = keys;
        this.values = values;
        this.valueTypes = valueTypes;
        this.flags = flags;
    }

    public HashMap<String, String> getKeys() {
        return this.keys;
    }

    public HashMap<String, String> getValues() {
        return values;
    }

    public HashMap<String, String> getValueTypes() {
        return valueTypes;
    }

    public String[] getFlags() {
        return flags;
    }
}
