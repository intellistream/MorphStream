package intellistream.morphstream.api.state;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.datatype.*;
import intellistream.morphstream.engine.txn.storage.table.RecordSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static intellistream.morphstream.engine.txn.storage.datatype.DataBox.Types.*;

public class DatabaseInitializer {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseInitializer.class);
    private Configuration configuration;
    private SpinLock[] spinlock;
    private String[] tableNames;
    private final HashMap<String, DataBox.Types> keyDataTypeMap = new HashMap<>();//table name to key data type
    private final HashMap<String, DataBox.Types[]> valuesDataTypeMap = new HashMap<>();//table name to value data type
    private final HashMap<String, String[]> fieldNamesMap = new HashMap<>();//table name to field names
    private final HashMap<String, RecordSchema> schemas = new HashMap<>();//table name to schema
    private int totalThreads;
    private final HashMap<String, Integer> numItemMaps = new HashMap<>();//table name to number of items
    public void creates_Table() {
        configure_db();
        for (String tableName : tableNames) {
            MorphStreamEnv.get().database().createTable(schemas.get(tableName), tableName, totalThreads, numItemMaps.get(tableName));
        }
    }
    public void loadDB(int threadId, SpinLock[] spinLocks) {
        for (String tableName : tableNames) {
            int interval = (int) Math.ceil(numItemMaps.get(tableName) / (double) totalThreads);
            int left_bound = threadId * interval;
            int right_bound;
            if (threadId == totalThreads - 1) {
                right_bound = numItemMaps.get(tableName);
            } else {
                right_bound = (threadId + 1) * interval;
            }
            int pid;
            String _key;
            for (int key = left_bound; key < right_bound; key++) {
                pid = get_pid(interval, key);
                _key = String.valueOf(key);
                insertRecord(tableName, _key, pid, spinLocks, pid);
            }
            LOG.info("Thread " + threadId + " loaded " + (right_bound - left_bound) + " records into table " + tableName);
        }
    }
    public void loadDB(int threadId, boolean isPartition) {//Used by SStore
        if (isPartition)
            loadDB(threadId, spinlock);
        else
            loadDB(threadId, null);
    }
    public void configure_db(){
        configuration = MorphStreamEnv.get().configuration();
        tableNames = configuration.getString("tableNames","table1,table2").split(",");
        totalThreads = configuration.getInt("loadDBThreadNum", 4);
        spinlock = new SpinLock[totalThreads];
        for (String tableName : tableNames) {
            numItemMaps.put(tableName, configuration.getInt(tableName + "_num_items", 1000000));
            keyDataTypeMap.put(tableName, getDataType(configuration.getString(tableName + "_key_data_types","string")));
            valuesDataTypeMap.put(tableName, getDataTypes(configuration.getString(tableName + "_value_data_types","int")));
            fieldNamesMap.put(tableName, configuration.getString(tableName + "_value_names","value").split(","));
            schemas.put(tableName, getRecordSchema(tableName));
        }
    }
    private void insertRecord(String tableName, String key, int pid, SpinLock[] spinLocks, int partition_id) {
        try {
            if (spinLocks != null)
                MorphStreamEnv.get().database().InsertRecord(tableName, getTableRecordWithSpinLock(tableName, key, pid, spinLocks), partition_id);
            else
                MorphStreamEnv.get().database().InsertRecord(tableName, getTableRecord(tableName, key), partition_id);
        } catch (DatabaseException e) {
            LOG.error("Error inserting record: " + e.getMessage());
        }
    }
    private TableRecord getTableRecord(String tableName, String key) {
        List<DataBox> values = new ArrayList<>();
        RecordSchema schema = schemas.get(tableName);
        values.add(new StringDataBox(key, key.length()));
        switch (schema.getFieldTypes().get(1).type()) {
            case INT:
                values.add(new IntDataBox(Integer.parseInt(key)));
                break;
            case LONG:
                values.add(new LongDataBox(Long.parseLong(key)));
                break;
            case FLOAT:
                values.add(new FloatDataBox(Float.parseFloat(key)));
                break;
            case DOUBLE:
                values.add(new DoubleDataBox(Double.parseDouble(key)));
                break;
            case BOOL:
                values.add(new BoolDataBox(Boolean.parseBoolean(key)));
                break;
            default:
                values.add(new StringDataBox(key));
        }
        return new TableRecord(new SchemaRecord(values), totalThreads);
    }
    private TableRecord getTableRecordWithSpinLock(String tableName, String key, int pid, SpinLock[] spinLocks) {
        List<DataBox> values = new ArrayList<>();
        RecordSchema schema = schemas.get(tableName);
        values.add(new StringDataBox(key, key.length()));
        switch (schema.getFieldTypes().get(1).type()) {
            case INT:
                values.add(new IntDataBox(Integer.parseInt(key)));
                break;
            case LONG:
                values.add(new LongDataBox(Long.parseLong(key)));
                break;
            case FLOAT:
                values.add(new FloatDataBox(Float.parseFloat(key)));
                break;
            case DOUBLE:
                values.add(new DoubleDataBox(Double.parseDouble(key)));
                break;
            case BOOL:
                values.add(new BoolDataBox(Boolean.parseBoolean(key)));
                break;
            default:
                values.add(new StringDataBox(key));
        }
        return new TableRecord(new SchemaRecord(values), pid, spinLocks);
    }

    private RecordSchema getRecordSchema(String tableName){
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        switch (keyDataTypeMap.get(tableName)){
            case INT:
                dataBoxes.add(new IntDataBox());
                break;
            case LONG:
                dataBoxes.add(new LongDataBox());
                break;
            case FLOAT:
                dataBoxes.add(new FloatDataBox());
                break;
            case DOUBLE:
                dataBoxes.add(new DoubleDataBox());
                break;
            case BOOL:
                dataBoxes.add(new BoolDataBox());
                break;
            default:
                dataBoxes.add(new StringDataBox());
        }
        for (DataBox.Types type : valuesDataTypeMap.get(tableName)){
            switch (type){
                case INT:
                    dataBoxes.add(new IntDataBox());
                    break;
                case LONG:
                    dataBoxes.add(new LongDataBox());
                    break;
                case FLOAT:
                    dataBoxes.add(new FloatDataBox());
                    break;
                case DOUBLE:
                    dataBoxes.add(new DoubleDataBox());
                    break;
                case BOOL:
                    dataBoxes.add(new BoolDataBox());
                    break;
                default:
                    dataBoxes.add(new StringDataBox());
            }
        }
        fieldNames.add("Key");//PK
        fieldNames.addAll(Arrays.asList(fieldNamesMap.get(tableName)));//values
        return new RecordSchema(fieldNames, dataBoxes);
    }
    private DataBox.Types getDataType (String type){
        switch (type){
            case "int":
                return INT;
            case "long":
                return LONG;
            case "float":
                return FLOAT;
            case "double":
                return DOUBLE;
            case "boolean":
                return BOOL;
            default:
                return STRING;
        }
    }
    private DataBox.Types[] getDataTypes (String typeArray) {
        String[] types = typeArray.split(",");
        DataBox.Types[] dataTypes = new DataBox.Types[types.length];
        for (int i = 0; i < types.length; i++) {
            switch (types[i]) {
                case "int":
                    dataTypes[i] = INT;
                    break;
                case "long":
                    dataTypes[i] = LONG;
                    break;
                case "float":
                    dataTypes[i] = FLOAT;
                    break;
                case "double":
                    dataTypes[i] = DOUBLE;
                    break;
                case "boolean":
                    dataTypes[i] = BOOL;
                    break;
                default:
                    dataTypes[i] = STRING;
            }
        }
        return dataTypes;
    }
    private int get_pid(int partition_interval, int key) {
        return (int) Math.floor(key / (double) partition_interval);//NUM_ITEMS / tthread;
    }
    public HashMap<String, Integer> getNumItemMaps() {
        return numItemMaps;
    }
    public void setSpinlock_(int i, SpinLock spinlock_) {
        this.spinlock[i] = spinlock_;
    }
    public HashMap<String, HashMap<String, Integer>> getTableFieldIndexMap() {
        HashMap<String, HashMap<String, Integer>> tableFieldIndexMap = new HashMap<>();
        for (String tableName : tableNames) {
            HashMap<String, Integer> fieldIndexMap = new HashMap<>();
            for (int i = 0; i < fieldNamesMap.get(tableName).length; i++) {
                fieldIndexMap.put(fieldNamesMap.get(tableName)[i], i + 1); //field index starts from 1
            }
            tableFieldIndexMap.put(tableName, fieldIndexMap);
        }
        return tableFieldIndexMap;
    }
}
