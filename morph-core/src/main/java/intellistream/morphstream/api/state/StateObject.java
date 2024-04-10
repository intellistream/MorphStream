package intellistream.morphstream.api.state;

import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class StateObject {
    @Getter
    private final String name;
    @Getter
    private final MetaTypes.AccessType type;
    @Getter
    private final String table;
    @Getter
    private final String key;
    @Setter
    private SchemaRecord schemaRecord; //one version of TableRecord
    private final HashMap<String, Integer> fieldNameToIndex; //table field name -> index, field index starts from 0

    public StateObject(String name, MetaTypes.AccessType type, String table, String key, HashMap<String, Integer> fieldNameToIndex) {
        this.name = name;
        this.type = type;
        this.table = table;
        this.key = key;
        this.fieldNameToIndex = fieldNameToIndex;
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
