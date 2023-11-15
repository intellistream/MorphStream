package intellistream.morphstream.api.state;


import intellistream.morphstream.api.utils.MetaTypes.AccessType;

import java.io.Serializable;

/**
 * Used as a data carrier for SchemaRecord, provides simplified retrieval-by-columnName APIs.
 */
public class StateObjectDescription implements Serializable {
    private final String name;
    private final AccessType type;
    private final int keyIndex;
    private final String tableName;
    private final int fieldIndex;

    public StateObjectDescription(String name, AccessType type, String tableName, int keyIndex, int fieldIndex) {
        this.name = name;
        this.tableName = tableName;
        this.type = type;
        this.keyIndex = keyIndex;
        this.fieldIndex = fieldIndex;
    }

    public String getName() {
        return name;
    }
    public int getKeyIndex() {
        return keyIndex;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public String getTableName() {
        return tableName;
    }

    public AccessType getType() {
        return type;
    }
}
