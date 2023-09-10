package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.ClientSideMetaTypes;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class StateObject {
    private final ClientSideMetaTypes.AccessType type;
    private final String table;
    private final String key;
    private final String value;
    private SchemaRecord schemaRecord;
    private HashMap<String, Integer> fieldNameToIndex; //TODO: Where to initialize?
    public StateObject(ClientSideMetaTypes.AccessType type, String table, String key, String value) {
        this.type = type;
        this.table = table;
        this.key = key;
        this.value = value;
    }
    /**
     * Called in Schedulers, set SchemaRecord
     */
    public void setSchemaRecord(SchemaRecord schemaRecord) {
        this.schemaRecord = schemaRecord;
    }

    public ClientSideMetaTypes.AccessType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public String getTable() {
        return table;
    }

    public String getValue() {
        return value;
    }

    public int getIntValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getInt();
    }
    public void setIntValue(String fieldName, int intValue) {
        schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).setInt(intValue);
    }
    public long getLongValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getLong();
    }
    public void setLongValue(String fieldName, long longValue) {
        schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).setLong(longValue);
    }
    public float getFloatValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getFloat();
    }
    public void setFloatValue(String fieldName, float floatValue) {
        schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).setFloat(floatValue);
    }
    public double getDoubleValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getDouble();
    }
    public void setDoubleValue(String fieldName, double doubleValue) {
        schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).setDouble(doubleValue);
    }
    public boolean getBoolValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getBool();
    }
    public void setBoolValue(String fieldName, boolean boolValue) {
        schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).setBool(boolValue);
    }
    public String getStringValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getString();
    }
    public void setStringValue(String fieldName, String stringValue) {
        schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).setString(stringValue, stringValue.length());
    }
    public HashSet getHashSetValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getHashSet();
    }
    public List<Double> getDoubleListValue(String fieldName) {
        return schemaRecord.getValues().get(fieldNameToIndex.get(fieldName)).getDoubleList();
    }
}
