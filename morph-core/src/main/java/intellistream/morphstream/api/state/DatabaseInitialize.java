package intellistream.morphstream.api.state;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.db.CavaliaDatabase;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.datatype.*;
import intellistream.morphstream.engine.txn.storage.table.RecordSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static intellistream.morphstream.engine.txn.storage.datatype.DataBox.Types.*;

public class DatabaseInitialize {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseInitialize.class);
    private Configuration configuration;
    private SpinLock[] spinlock;
    private String[] tableNames;
    private HashMap<String, DataBox.Types> keyDataTypeMap = new HashMap<>();//table name to key data type
    private HashMap<String, DataBox.Types> valueDataTypeMap = new HashMap<>();//table name to value data type
    private HashMap<String, RecordSchema> schemas = new HashMap<>();//table name to schema
    private int totalThreads;
    private HashMap<String, Integer> numItemMaps = new HashMap<>();//table name to number of items
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
        totalThreads = configuration.getInt("tthread", 4);
        spinlock = new SpinLock[totalThreads];
        for (String tableName : tableNames) {
            numItemMaps.put(tableName, configuration.getInt(tableName + "_num_items", 1000000));
            keyDataTypeMap.put(tableName, getDataType(configuration.getString(tableName + "_key_data_types","string")));
            valueDataTypeMap.put(tableName, getDataType(configuration.getString(tableName + "_value_data_types","int")));
            schemas.put(tableName, getRecordSchema(tableName));
        }
    }
    private void insertRecord(String tableName, String key, int pid, SpinLock[] spinLocks, int partition_id) {
        try {
            if (spinLocks != null)
                MorphStreamEnv.get().database().InsertRecord("bookEntries", getTableRecordWithSpinLock(tableName, key, pid, spinLocks), partition_id);
            else
                MorphStreamEnv.get().database().InsertRecord("bookEntries", getTableRecord(tableName, key), partition_id);
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
            default:
                dataBoxes.add(new StringDataBox());
        }
        switch (valueDataTypeMap.get(tableName)){
            case INT:
                dataBoxes.add(new IntDataBox());
                break;
            case LONG:
                dataBoxes.add(new LongDataBox());
                break;
            case FLOAT:
                dataBoxes.add(new FloatDataBox());
                break;
            default:
                dataBoxes.add(new StringDataBox());
        }
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
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
            default:
                return STRING;
        }
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
}
