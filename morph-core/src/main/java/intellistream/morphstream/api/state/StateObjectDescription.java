package intellistream.morphstream.api.state;


import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Used as a data carrier for SchemaRecord, provides simplified retrieval-by-columnName APIs.
 */
public class StateObjectDescription {
    private final AccessType type;
    private final int keyIndex;
    private final String tableName;
    private final String keyName;
    private final String valueName;

    public StateObjectDescription(AccessType type, String tableName, String keyName, String valueName, int keyIndex) {
        this.tableName = tableName;
        this.keyName = keyName;
        this.type = type;
        this.valueName = valueName;
        this.keyIndex = keyIndex;
    }

    public String getValueName() {
        return valueName;
    }

    public String getKeyName() {
        return keyName;
    }
    public int getKeyIndex() {
        return keyIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public AccessType getType() {
        return type;
    }
}
