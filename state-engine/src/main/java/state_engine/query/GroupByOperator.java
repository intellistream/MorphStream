package state_engine.query;
import state_engine.DatabaseException;
import state_engine.SimpleDatabase;
import state_engine.storage.MarkerRecord;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.DataBox;
import state_engine.storage.table.RecordSchema;
import state_engine.storage.table.stats.TableStats;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
public class GroupByOperator extends QueryOperator {
    private int groupByColumnIndex;
    private String groupByColumn;
    private SimpleDatabase.Transaction transaction;
    /**
     * Create a new GroupByOperator that pulls from source and groups by groupByColumn.
     *
     * @param source        the source operator of this operator
     * @param transaction   the transaction containing this operator
     * @param groupByColumn the column to group on
     * @throws QueryPlanException
     */
    public GroupByOperator(QueryOperator source,
                           SimpleDatabase.Transaction transaction,
                           String groupByColumn) throws QueryPlanException {
        super(OperatorType.GROUPBY, source);
        RecordSchema sourceSchema = this.getSource().getOutputSchema();
        this.transaction = transaction;
        this.groupByColumn = this.checkSchemaForColumn(sourceSchema, groupByColumn);
        this.groupByColumnIndex = sourceSchema.getFieldNames().indexOf(this.groupByColumn);
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }
    public Iterator<SchemaRecord> iterator() throws QueryPlanException, DatabaseException {
        return new GroupByIterator();
    }
    protected RecordSchema computeSchema() throws QueryPlanException {
        return this.getSource().getOutputSchema();
    }
    public String str() {
        return "type: " + this.getType() +
                "\ncolumn: " + this.groupByColumn;
    }
    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    public TableStats estimateStats() throws QueryPlanException {
        return this.getSource().getStats();
    }
    public int estimateIOCost() throws QueryPlanException {
        return this.getSource().getIOCost();
    }
    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class GroupByIterator implements Iterator<SchemaRecord> {
        private Iterator<SchemaRecord> sourceIterator;
        private MarkerRecord markerRecord;
        private Map<String, String> hashGroupTempTables;
        private int currCount;
        private Iterator<String> keyIter;
        private Iterator<SchemaRecord> rIter;
        public GroupByIterator() throws QueryPlanException, DatabaseException {
            this.sourceIterator = GroupByOperator.this.getSource().iterator();
            this.markerRecord = MarkerRecord.getMarker();
            this.hashGroupTempTables = new HashMap<>();
            this.currCount = 0;
            this.rIter = null;
            while (this.sourceIterator.hasNext()) {
                SchemaRecord record = this.sourceIterator.next();
                DataBox groupByColumn = record.getValues().get(GroupByOperator.this.groupByColumnIndex);
                String tableName;
                if (!this.hashGroupTempTables.containsKey(groupByColumn.toString())) {
                    tableName = "Temp" + GroupByOperator.this.groupByColumn + "GroupBy" + this.hashGroupTempTables.size();
                    GroupByOperator.this.transaction.createTempTable(GroupByOperator.this.getSource().getOutputSchema(), tableName);
                    this.hashGroupTempTables.put(groupByColumn.toString(), tableName);
                } else {
                    tableName = this.hashGroupTempTables.get(groupByColumn.toString());
                }
                GroupByOperator.this.transaction.addRecord(tableName, record);
            }
            this.keyIter = hashGroupTempTables.keySet().iterator();
        }
        /**
         * Checks if there are more d_record(s) to yield
         *
         * @return true if this iterator has another d_record to yield, otherwise false
         */
        public boolean hasNext() {
            return this.keyIter.hasNext() || (this.rIter != null && this.rIter.hasNext());
        }
        /**
         * Yields the next d_record of this iterator.
         *
         * @return the next SchemaRecord
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public SchemaRecord next() {
            while (this.hasNext()) {
                if (this.rIter != null && this.rIter.hasNext()) {
                    return this.rIter.next();
                } else if (this.keyIter.hasNext()) {
                    String key = this.keyIter.next();
                    String tableName = this.hashGroupTempTables.get(key);
                    Iterator<SchemaRecord> prevIter = this.rIter;
                    try {
                        this.rIter = GroupByOperator.this.transaction.getRecordIterator(tableName);
                    } catch (DatabaseException de) {
                        throw new NoSuchElementException();
                    }
                    if (prevIter != null && ++this.currCount < this.hashGroupTempTables.size()) {
                        return markerRecord;
                    }
                }
            }
            throw new NoSuchElementException();
        }
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
