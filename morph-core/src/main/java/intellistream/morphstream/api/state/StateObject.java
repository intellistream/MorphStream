package intellistream.morphstream.api.state;


import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Used as a data carrier for SchemaRecord, provides simplified retrieval-by-columnName APIs.
 */
public class StateObject {
    private final String tableName;
    private final String keyName;
    private final AccessType type;
    private SchemaRecord schemaRecord;
    private HashMap<String, Integer> fieldNameToIndex; //TODO: Where to initialize?

    public StateObject(String tableName, String keyName, AccessType type) {
        this.tableName = tableName;
        this.keyName = keyName;
        this.type = type;
    }

    /**
     * Called in Schedulers, set SchemaRecord
     */
    public void setSchemaRecord(SchemaRecord schemaRecord) {
        this.schemaRecord = schemaRecord;
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
