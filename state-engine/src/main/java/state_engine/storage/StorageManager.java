package state_engine.storage;
import state_engine.db.DatabaseException;
import state_engine.storage.datatype.DataBox;
import state_engine.storage.table.BaseTable;
import state_engine.storage.table.RecordSchema;
import state_engine.storage.table.ShareTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
public class StorageManager {
    public Map<String, BaseTable> tables;
    int table_count;
    public StorageManager() {
        tables = new ConcurrentHashMap<>();
    }
    public BaseTable getTable(String tableName) throws DatabaseException {
        if (!tables.containsKey(tableName)) {
            throw new DatabaseException("Table: " + tableName + " does not exist");
        }
        return tables.get(tableName);
    }
    public void InsertRecord(String tableName, TableRecord record) throws DatabaseException {
        BaseTable tab = getTable(tableName);
        tab.InsertRecord(record);
    }
    /**
     * Create a new table in this database.
     *
     * @param s         the table schema
     * @param tableName the name of the table
     * @throws DatabaseException
     */
    public synchronized void createTable(RecordSchema s, String tableName) throws DatabaseException {
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("Table name already exists");
        }
        tables.put(tableName, new ShareTable(s, tableName, true));//here we decide which table to use.
        table_count++;
    }
    /**
     * TODO: to be implemented.
     * Create a new table in this database with an index on each of the given column names.
     * NOTE: YOU CAN NOT DELETE/UPDATE FROM THIS TABLE IF YOU CHOOSE TO BUILD INDICES!!
     *
     * @param s            the table schema
     * @param tableName    the name of the table
     * @param indexColumns the list of unique columnNames on the maintain an index on
     * @throws DatabaseException
     */
    public synchronized void createTableWithIndices(RecordSchema s, String tableName, List<String> indexColumns) throws DatabaseException {
        if (tables.containsKey(tableName)) {
            throw new DatabaseException("SimpleTable name already exists");
        }
        List<String> schemaColNames = s.getFieldNames();
        List<DataBox> schemaColType = s.getFieldTypes();
        HashSet<String> seenColNames = new HashSet<>();
        List<Integer> schemaColIndex = new ArrayList<>();
        for (String col : indexColumns) {
            if (!schemaColNames.contains(col)) {
                throw new DatabaseException("Column desired for index does not exist");
            }
            if (seenColNames.contains(col)) {
                throw new DatabaseException("Column desired for index has been duplicated");
            }
            seenColNames.add(col);
            schemaColIndex.add(schemaColNames.indexOf(col));
        }
        tables.put(tableName, new ShareTable(s, tableName, true));
        for (int i : schemaColIndex) {
            String colName = schemaColNames.get(i);
            DataBox colType = schemaColType.get(i);
            String indexName = tableName + "," + colName;
            //this.indexLookup.put(indexName, new BtreeIndex(colType, indexName, this.fileDir));
        }
    }
    /**
     * Delete a table in this database.
     *
     * @param tableName the name of the table
     * @return true if the database was successfully deleted
     */
    public synchronized boolean dropTable(String tableName) throws IOException {
        if (!tables.containsKey(tableName)) {
            return false;
        }
        tables.get(tableName).close();
        tables.remove(tableName);
        return true;
    }
    /**
     * Delete all tables from this database.
     */
    public synchronized void dropAllTables() throws IOException {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        for (String s : tableNames) {
            dropTable(s);
        }
    }
    /**
     * Close this database.
     */
    public synchronized void close() throws IOException {
        for (BaseTable t : tables.values()) {
            t.close();
        }
        tables.clear();
    }
}
