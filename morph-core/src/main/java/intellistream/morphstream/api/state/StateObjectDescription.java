package intellistream.morphstream.api.state;


import intellistream.morphstream.api.utils.MetaTypes.AccessType;

/**
 * Used as a data carrier for SchemaRecord, provides simplified retrieval-by-columnName APIs.
 */
public class StateObjectDescription {
    private final String name;
    private final AccessType type;
    private final int keyIndex;
    private final String tableName;
    private final String keyName;
    private final String valueName;

    public StateObjectDescription(String name, AccessType type, String tableName, String keyName, String valueName, int keyIndex) {
        this.name = name;
        this.tableName = tableName;
        this.keyName = keyName;
        this.type = type;
        this.valueName = valueName;
        this.keyIndex = keyIndex;
    }

    public String getName() {
        return name;
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
