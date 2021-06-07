package storage.table;

import db.DatabaseException;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import storage.store.Store;
import storage.store.memory.SimpleStore;
import storage.table.stats.TableStats;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A database table. Allows the user to add, delete, update, and get records.
 * A table has an associated schema, stats, and page allocator. The first page
 * in the page allocator is a header page that serializes the schema, and each
 * subsequent page is a data page containing the table records.
 * <p>
 * Properties:
 * `schema`: the RecordSchema (column names and column types) for this table
 * `stats`: the TableStats for this table
 * `tableName`: name of this table
 * `numRecords`: number of records currently contained in this table
 */
public class SimpleTable {
    public static final String FILENAME_PREFIX = "db";
    private final RecordSchema schema;
    private final Store store;
    private final TableStats stats;
    private final String tableName;
    private int numRecords;

    //TODO: move index structure here.
    //A table may or may not have index associated. The index structure should not be maintained outside.
    public SimpleTable(RecordSchema schema, String tableName) {
        this.schema = schema;
        this.tableName = tableName;
        this.stats = new TableStats(this.schema);
        store = new SimpleStore();
    }

    public void close() {
        store.clean();
    }

    public Iterator<SchemaRecord> iterator() {
        return new TableIterator();
    }

    /**
     * Adds a new d_record to this table.
     *
     * @param row the row to be added.
     * @return the RowID of the added d_record
     * @throws DatabaseException if the values passed in to this method do not
     *                           correspond to the schema of this table
     */
    public RowID addRecord(SchemaRecord row) throws DatabaseException {
        SchemaRecord record;
        try {
            record = this.schema.verify(row.getValues());
        } catch (SchemaException se) {
            throw new DatabaseException(se.getMessage());
        }
        this.numRecords++;
        final RowID rowID = new RowID(numRecords);
        store.addrow(rowID, row);
        this.stats.addRecord(record);
        return rowID;
    }

    /**
     * Delete all records in the table.
     */
    public void clean() {
        this.numRecords = 0;
        store.clean();
        this.stats.clean();
    }

    /**
     * Deletes the d_record specified by rid from the table.
     *
     * @param rid the RowID of the d_record to delete
     * @return the SchemaRecord referenced by rid that was removed
     * @throws DatabaseException if rid does not correspond to a valid d_record
     */
    public SchemaRecord deleteRecord(RowID rid) {
        this.numRecords--;
        final SchemaRecord oldRecord = store.deleterow(rid);
        this.stats.removeRecord(oldRecord);
        return oldRecord;
    }

    /**
     * Retrieves a d_record from the table.
     *
     * @param rid the RowID of the d_record to retrieve
     * @return the SchemaRecord referenced by rid
     * @throws DatabaseException if rid does not correspond to a valid d_record
     */
    public SchemaRecord getRecord(RowID rid) {
        return store.getrow(rid);
    }

    /**
     * Updates an existing d_record with new values and returns the old version of the d_record.
     * Make sure to update this.stats as necessary.
     *
     * @param values the new values of the d_record
     * @param rid    the RowID of the d_record to update
     * @return the old version of the d_record
     * @throws DatabaseException if rid does not correspond to a valid d_record or
     *                           if the values do not correspond to the schema of this table
     */
    public SchemaRecord updateRecord(List<DataBox> values, RowID rid) throws DatabaseException {
        SchemaRecord record;
        try {
            record = this.schema.verify(values);
        } catch (SchemaException se) {
            throw new DatabaseException(se.getMessage());
        }
        SchemaRecord oldRecord = store.updaterow(record, rid);
        this.stats.removeRecord(oldRecord);
        return oldRecord;
    }

    public long getNumRecords() {
        return this.numRecords;
    }

    public RecordSchema getSchema() {
        return this.schema;
    }

    public TableStats getStats() {
        return this.stats;
    }

    public int getEntrySize() {
        return this.schema.getEntrySize();
    }

    /**
     * An implementation of Iterator that provides an iterator interface over all
     * of the records in this table.
     */
    private class TableIterator implements Iterator<SchemaRecord> {
        private final Iterator<SchemaRecord> rowIter;
        private long recordCount;

        public TableIterator() {
            this.rowIter = store.getRowIterator();
        }

        /**
         * Checks if there are more d_record(s) to yield
         *
         * @return true if this iterator has another d_record to yield, otherwise false
         */
        public boolean hasNext() {
            return this.recordCount < numRecords;
        }

        /**
         * Yields the next d_record of this iterator.
         *
         * @return the next SchemaRecord
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public SchemaRecord next() {
            if (hasNext()) {
                this.recordCount++;
            }
            return rowIter.next();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
