package intellistream.morphstream.engine.txn.operator;

import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.impl.SimpleDatabase;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.db.storage.table.RecordSchema;
import intellistream.morphstream.engine.db.storage.table.stats.TableStats;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class IndexScanOperator extends QueryOperator {
    private final SimpleDatabase.Transaction transaction;
    private final String tableName;
    private final String columnName;
    private final QueryPlan.PredicateOperator predicate;
    private final DataBox value;
    private final int columnIndex;

    /**
     * An index scan operator.
     *
     * @param transaction the transaction containing this operator
     * @param tableName   the table to iterate over
     * @param columnName  the name of the column the index is on
     * @throws QueryPlanException
     * @throws DatabaseException
     */
    public IndexScanOperator(SimpleDatabase.Transaction transaction,
                             String tableName,
                             String columnName,
                             QueryPlan.PredicateOperator predicate,
                             DataBox value) throws QueryPlanException {
        super(OperatorType.INDEXSCAN);
        this.tableName = tableName;
        this.transaction = transaction;
        this.columnName = columnName;
        this.predicate = predicate;
        this.value = value;
        this.setOutputSchema(this.computeSchema());
        columnName = this.checkSchemaForColumn(this.getOutputSchema(), columnName);
        this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(columnName);
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public String str() {
        return "type: " + this.getType() +
                "\ntable: " + this.tableName +
                "\ncolumn: " + this.columnName +
                "\noperator: " + this.predicate +
                "\nvalue_list: " + this.value;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    public TableStats estimateStats() throws QueryPlanException {
        TableStats stats;
        try {
            stats = this.transaction.getStats(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
        return stats.copyWithPredicate(this.columnIndex,
                this.predicate,
                this.value);
    }

    /**
     * Estimates the IO cost of executing this query operator.
     * You should calculate this estimate cost with the formula
     * taught to you in class. Note that the index you've implemented
     * in this project is an unclustered index.
     * <p>
     * You will find the following instance variables helpful:
     * this.transaction, this.tableName, this.columnName,
     * this.columnIndex, this.predicate, and this.value_list.
     * <p>
     * You will find the following methods helpful: this.transaction.getStats,
     * this.transaction.getNumRecords, this.transaction.getNumIndexPages,
     * and tableStats.getReductionFactor.
     *
     * @return estimate IO cost
     * @throws QueryPlanException
     */
    public int estimateIOCost() {
        /* TODO: Implement me! */
//		int io;
//		try {
//			TableStats t = this.transaction.getStats(this.tableName);
//			long tuples = this.transaction.getNumRecords(this.tableName);
//			int pages = this.transaction.getNumIndexPages(this.tableName, this.columnName);
//			float rf = t.getReductionFactor(this.columnIndex, this.predicate, this.value_list);
//			io = (int) Math.ceil((double) ((pages + tuples) * rf));
//		} catch (DatabaseException e) {
        return 0;
//		}
//		return io;
    }

    public Iterator<SchemaRecord> iterator() {
        return new IndexScanIterator();
    }

    public RecordSchema computeSchema() throws QueryPlanException {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class IndexScanIterator implements Iterator<SchemaRecord> {
        private Iterator<SchemaRecord> sourceIterator;
        private SchemaRecord nextRecord;

        public IndexScanIterator() {
            this.nextRecord = null;
//			if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.EQUALS) {
//				this.sourceIterator = IndexScanOperator.this.transaction.lookupKey(
//						IndexScanOperator.this.tableName,
//						IndexScanOperator.this.columnName,
//						IndexScanOperator.this.value_list);
//			} else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN ||
//					IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN_EQUALS) {
//				this.sourceIterator = IndexScanOperator.this.transaction.sortedScan(
//						IndexScanOperator.this.tableName,
//						IndexScanOperator.this.columnName);
//			} else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.GREATER_THAN) {
//				this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
//						IndexScanOperator.this.tableName,
//						IndexScanOperator.this.columnName,
//						IndexScanOperator.this.value_list);
//				while (this.sourceIterator.hasNext()) {
//					SchemaRecord r = this.sourceIterator.next();
//
//					if (r.getValues().get(IndexScanOperator.this.columnIndex)
//							.compareTo(IndexScanOperator.this.value_list) > 0) {
//						this.nextRecord = r;
//						break;
//					}
//				}
//			} else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.GREATER_THAN_EQUALS) {
//				this.sourceIterator = IndexScanOperator.this.transaction.sortedScanFrom(
//						IndexScanOperator.this.tableName,
//						IndexScanOperator.this.columnName,
//						IndexScanOperator.this.value_list);
//			}
        }

        /**
         * Checks if there are more d_record(s) to yield
         *
         * @return true if this iterator has another d_record to yield, otherwise false
         */
        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN) {
                if (this.sourceIterator.hasNext()) {
                    SchemaRecord r = this.sourceIterator.next();
                    if (r.getValues().get(IndexScanOperator.this.columnIndex)
                            .compareTo(IndexScanOperator.this.value) >= 0) {
                        return false;
                    }
                    this.nextRecord = r;
                    return true;
                }
                return false;
            } else if (IndexScanOperator.this.predicate == QueryPlan.PredicateOperator.LESS_THAN_EQUALS) {
                if (this.sourceIterator.hasNext()) {
                    SchemaRecord r = this.sourceIterator.next();
                    if (r.getValues().get(IndexScanOperator.this.columnIndex)
                            .compareTo(IndexScanOperator.this.value) > 0) {
                        return false;
                    }
                    this.nextRecord = r;
                    return true;
                }
                return false;
            }
            if (this.sourceIterator.hasNext()) {
                this.nextRecord = this.sourceIterator.next();
                return true;
            }
            return false;
        }

        /**
         * Yields the next d_record of this iterator.
         *
         * @return the next SchemaRecord
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public SchemaRecord next() {
            if (this.hasNext()) {
                SchemaRecord r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}