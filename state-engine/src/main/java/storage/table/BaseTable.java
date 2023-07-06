package storage.table;

import db.DatabaseException;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.table.stats.TableStats;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A database table. Allows the user to addOperation, delete, update, and get records.
 * A table has an associated schema, stats, and page allocator. The first page
 * in the page allocator is a header page that serializes the schema, and each
 * subsequent page is a data page containing the table records.
 * <p>
 * Properties:
 * `schema`: the RecordSchema (column names and column types) for this table
 * `stats`: the TableStats for this table
 * `table_Id`: name of this table
 * `numRecords`: number of records currently contained in this table
 */
public abstract class BaseTable implements ITable {
    public static final String FILENAME_PREFIX = "db";
    final AtomicInteger numRecords = new AtomicInteger();
    final int secondary_count_;
    private final RecordSchema schema;
    private final TableStats stats;
    private final String table_Id;

    //TODO: move index structure here.
    //A table may or may not have index associated. The index structure should not be maintained outside.
    public BaseTable(RecordSchema schema, String table_Id) {
        this.schema = schema;
        this.table_Id = table_Id;
        this.stats = new TableStats(this.schema);
        secondary_count_ = schema.getSecondary_num_();
    }

    /**
     * Adds a new d_record to this table.
     *
     * @param record the row to be added.
     * @return the RowID of the added d_record
     * @throws DatabaseException if the values passed in to this method do not
     *                           correspond to the schema of this table
     */
    public abstract boolean InsertRecord(TableRecord record, int partition_id) throws DatabaseException;
    public abstract boolean InsertRecord(TableRecord record) throws DatabaseException;
    public abstract HashMap<String, TableRecord> getTableIndexByPartitionId(int partitionId);

    /**
     * Delete all records in the table.
     */
    public abstract void clean();

    /**
     * Deletes the d_record specified by rid from the table.
     *
     * @param rid the RowID of the d_record to delete
     * @return the SchemaRecord referenced by rid that was removed
     * @throws DatabaseException if rid does not correspond to a valid d_record
     */
    public abstract SchemaRecord deleteRecord(RowID rid);

    /**
     * Retrieves a d_record from the table.
     *
     * @param rid the RowID of the d_record to retrieve
     * @return the SchemaRecord referenced by rid
     * @throws DatabaseException if rid does not correspond to a valid d_record
     */
    public abstract SchemaRecord getRecord(RowID rid);

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
    public abstract SchemaRecord updateRecord(List<DataBox> values, RowID rid);

    public int getNumRecords() {
        return this.numRecords.get();
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
}
