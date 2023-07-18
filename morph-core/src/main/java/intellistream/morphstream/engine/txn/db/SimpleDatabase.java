package intellistream.morphstream.engine.txn.db;

import intellistream.morphstream.engine.txn.content.common.TxnParam;
import intellistream.morphstream.engine.txn.operator.QueryPlan;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.storage.table.RowID;
import intellistream.morphstream.engine.txn.storage.table.SimpleTable;
import intellistream.morphstream.engine.txn.storage.table.stats.TableStats;

import java.util.*;

/**
 * Simply put locks on tables for each transaction.
 * This could be useful to examine CC with streaming (remove all locks.) instead of CC with DB.
 */
public class SimpleDatabase {
    private final LockManager lockMan;
    public TxnParam param;
    public Map<String, SimpleTable> tables;
    private long numTransactions;

    /**
     * Creates a new database.
     *
     * @param fileDir the directory to put the table files in, not in use currently.
     * @throws DatabaseException
     */
    public SimpleDatabase(String fileDir) {
        numTransactions = 0;
        lockMan = new LockManager();
    }

    /**
     * Start a new transaction.
     *
     * @return the new Transaction
     */
    public synchronized Transaction beginTransaction() {
        Transaction t = new Transaction(this.numTransactions);
        this.numTransactions++;
        return t;
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
        tables.put(tableName, new SimpleTable(s, tableName));
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
    public synchronized boolean dropTable(String tableName) {
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
    public synchronized void dropAllTables() {
        List<String> tableNames = new ArrayList<>(tables.keySet());
        for (String s : tableNames) {
            dropTable(s);
        }
    }

    /**
     * Close this database.
     */
    public synchronized void close() {
        for (SimpleTable t : tables.values()) {
            t.close();
        }
        tables.clear();
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
            throw new DatabaseException("SimpleTable name already exists");
        }
        tables.put(tableName, new SimpleTable(s, tableName));
    }

    public class Transaction {
        long transNum;
        boolean active;
        HashMap<String, LockManager.LockType> locksHeld;
        HashMap<String, SimpleTable> tempTables;
        HashMap<String, String> aliasMaps;

        private Transaction(long tNum) {
            this.transNum = tNum;
            this.active = true;
            this.locksHeld = new HashMap<>();
            this.tempTables = new HashMap<>();
            this.aliasMaps = new HashMap<>();
        }

        public boolean isActive() {
            return this.active;
        }

        public void end() {
            assert (this.active);
            releaseAllLocks();
            deleteAllTempTables();
            this.active = false;
        }

        /**
         * Allows the user to query a table. See query#QueryPlan
         *
         * @param tableName The name/alias of the table wished to be queried.
         * @throws DatabaseException if table does not exist
         */
        public QueryPlan query(String tableName) throws DatabaseException {
            assert (this.active);
            checkAndGrabSharedLock(tableName);
            return new QueryPlan(this, tableName);
        }

        /**
         * Allows the user to provide an alias for a particular table. That alias is valid for the
         * remainder of the transaction. For a particular QueryPlan, once you specify an alias, you
         * must use that alias for the rest of the query.
         *
         * @param tableName The original name of the table.
         * @param alias     The new Aliased name.
         * @throws DatabaseException if the alias already exists or the table does not.
         */
        public void queryAs(String tableName, String alias) throws DatabaseException {
            assert (this.active);
            if (tables.containsKey(alias)
                    || this.tempTables.containsKey(alias)
                    || this.aliasMaps.containsKey(alias)) {
                throw new DatabaseException("SimpleTable name already exists");
            }
            checkAndGrabSharedLock(tableName);
            if (tables.containsKey(tableName)) {
                this.aliasMaps.put(alias, tableName);
            } else if (tempTables.containsKey(tableName)) {
                this.aliasMaps.put(alias, tableName);
            } else {
                throw new DatabaseException("SimpleTable name not found");
            }
        }

        /**
         * Create a temporary table within this transaction.
         *
         * @param schema        the table schema
         * @param tempTableName the name of the table
         * @throws DatabaseException
         */
        public void createTempTable(RecordSchema schema, String tempTableName) throws DatabaseException {
            assert (this.active);
            if (tables.containsKey(tempTableName)
                    || this.tempTables.containsKey(tempTableName)) {
                throw new DatabaseException("SimpleTable name already exists");
            }
            this.tempTables.put(tempTableName, new SimpleTable(schema, tempTableName));
            this.locksHeld.put(tempTableName, LockManager.LockType.EXCLUSIVE);
        }

        /**
         * Perform a check to see if the database has an index on this (table,column).
         *
         * @param tableName  the name of the table
         * @param columnName the name of the column
         * @return boolean if the index exists
         */
        public boolean indexExists(String tableName, String columnName) {
//			try {
//				resolveIndexFromName(tableName, columnName);
//			} catch (DatabaseException e) {
            return false;
//			}
//			return true;
        }

        public RowID addRecord(String tableName, SchemaRecord row) throws DatabaseException {
            assert (this.active);
            checkAndGrabExclusiveLock(tableName);
            SimpleTable tab = getTable(tableName);
            RowID rid = tab.addRecord(row);
            row.setID(rid);

            return rid;
        }

        /**
         * Delete all records.
         *
         * @param tableName
         */
        public void deleteRecord(String tableName) throws DatabaseException {
            assert (active);
            checkAndGrabExclusiveLock(tableName);
            SimpleTable tab = getTable(tableName);
            tab.clean();
        }

        /**
         * Delete a row form the table.
         *
         * @param tableName
         * @param rid
         * @throws DatabaseException
         */
        public void deleteRecord(String tableName, RowID rid) throws DatabaseException {
            assert (active);
            checkAndGrabExclusiveLock(tableName);
            SimpleTable tab = getTable(tableName);
            RecordSchema s = tab.getSchema();
            SchemaRecord rec = tab.deleteRecord(rid);

        }

        public SchemaRecord getRecord(String tableName, RowID rid) throws DatabaseException {
            assert (active);
            checkAndGrabSharedLock(tableName);
            return getTable(tableName).getRecord(rid);
        }

        public Iterator<SchemaRecord> getRecordIterator(String tableName) throws DatabaseException {
            assert (this.active);
            checkAndGrabSharedLock(tableName);
            return getTable(tableName).iterator();
        }

        /**
         * @param tableName the table to be updated.
         * @param values    the new values of the d_record
         * @param rid       the RowID of the d_record to update
         * @throws DatabaseException
         */
        public void updateRecord(String tableName, List<DataBox> values, RowID rid) throws DatabaseException {
            assert (this.active);
            checkAndGrabExclusiveLock(tableName);
            SimpleTable tab = getTable(tableName);
            RecordSchema s = tab.getSchema();
            SchemaRecord rec = tab.updateRecord(values, rid);
            List<DataBox> oldValues = rec.getValues();
            List<String> colNames = s.getFieldNames();
            for (String col : colNames) {
                if (indexExists(tableName, col)) {
//					BtreeIndex tree = resolveIndexFromName(tableName, col);
//					tree.deleteKey(oldValues.get(i), rid);
//					tree.insertKey(values.get(i), rid);
                }
            }
        }

        public TableStats getStats(String tableName) throws DatabaseException {
            assert (this.active);
            checkAndGrabSharedLock(tableName);
            return getTable(tableName).getStats();
        }

        public int getEntrySize(String tableName) throws DatabaseException {
            assert (this.active);
            checkAndGrabSharedLock(tableName);
            return getTable(tableName).getEntrySize();
        }

        public long getNumRecords(String tableName) throws DatabaseException {
            assert (this.active);
            checkAndGrabSharedLock(tableName);
            return getTable(tableName).getNumRecords();
        }

        public RecordSchema getSchema(String tableName) throws DatabaseException {
            assert (this.active);
            checkAndGrabSharedLock(tableName);
            return getTable(tableName).getSchema();
        }

        public RecordSchema getFullyQualifiedSchema(String tableName) throws DatabaseException {
            assert (this.active);
            checkAndGrabSharedLock(tableName);
            RecordSchema schema = getTable(tableName).getSchema();
            List<String> newColumnNames = new ArrayList<>();
            for (String oldName : schema.getFieldNames()) {
                newColumnNames.add(tableName + "." + oldName);
            }
            return new RecordSchema(newColumnNames, schema.getFieldTypes());
        }

        private SimpleTable getTable(String tableName) throws DatabaseException {
            if (this.tempTables.containsKey(tableName)) {
                return this.tempTables.get(tableName);
            }
            while (aliasMaps.containsKey(tableName)) {
                tableName = aliasMaps.get(tableName);
            }
            if (!tables.containsKey(tableName)) {
                throw new DatabaseException("SimpleTable: " + tableName + "does not exist");
            }
            checkAndGrabSharedLock(tableName);
            return tables.get(tableName);
        }

        private void checkAndGrabSharedLock(String tableName) throws DatabaseException {
            if (this.locksHeld.containsKey(tableName)) {
                return;
            }
            while (aliasMaps.containsKey(tableName)) {
                tableName = aliasMaps.get(tableName);
            }
            if (!this.tempTables.containsKey(tableName) && !tables.containsKey(tableName)) {
                throw new DatabaseException("SimpleTable: " + tableName + " Does not exist");
            }
            //TODO: it's now at table level.
            LockManager lockMan = SimpleDatabase.this.lockMan;
            if (lockMan.holdsLock(tableName, this.transNum, LockManager.LockType.SHARED)) {
                this.locksHeld.put(tableName, LockManager.LockType.SHARED);
            } else {
                lockMan.acquireLock(tableName, this.transNum, LockManager.LockType.SHARED);
            }
        }

        private void checkAndGrabExclusiveLock(String tableName) throws DatabaseException {
            while (aliasMaps.containsKey(tableName)) {
                tableName = aliasMaps.get(tableName);
            }
            if (this.locksHeld.containsKey(tableName) && this.locksHeld.get(tableName).equals(LockManager.LockType.EXCLUSIVE)) {
                return;
            }
            if (!this.tempTables.containsKey(tableName) && !tables.containsKey(tableName)) {
                throw new DatabaseException("SimpleTable: " + tableName + " Does not exist");
            }
            LockManager lockMan = SimpleDatabase.this.lockMan;
            if (lockMan.holdsLock(tableName, this.transNum, LockManager.LockType.EXCLUSIVE)) {
                this.locksHeld.put(tableName, LockManager.LockType.EXCLUSIVE);
            } else {
                lockMan.acquireLock(tableName, this.transNum, LockManager.LockType.EXCLUSIVE);
            }
        }

        private void releaseAllLocks() {
            LockManager lockMan = SimpleDatabase.this.lockMan;
            for (String tableName : this.locksHeld.keySet()) {
                lockMan.releaseLock(tableName, this.transNum);
            }
        }

        public void deleteTempTable(String tempTableName) {
            assert (this.active);
            if (!this.tempTables.containsKey(tempTableName)) {
                return;
            }
            this.tempTables.get(tempTableName).close();
            tables.remove(tempTableName);
        }

        private void deleteAllTempTables() {
            Set<String> keys = tempTables.keySet();
            for (String tableName : keys) {
                deleteTempTable(tableName);
            }
        }
    }
}
