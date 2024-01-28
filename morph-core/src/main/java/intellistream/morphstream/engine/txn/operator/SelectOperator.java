package intellistream.morphstream.engine.txn.operator;

import intellistream.morphstream.engine.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.MarkerRecord;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.storage.table.stats.TableStats;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SelectOperator extends QueryOperator {
    private final int columnIndex;
    private final String columnName;
    private final QueryPlan.PredicateOperator operator;
    private final DataBox value;

    /**
     * Creates a new SelectOperator that pulls from source and only returns tuples for which the
     * predicate is satisfied.
     *
     * @param source     the source of this operator
     * @param columnName the name of the column to evaluate the predicate on
     * @param operator   the actual comparator
     * @param value      the value_list to compare against
     * @throws QueryPlanException
     */
    public SelectOperator(QueryOperator source,
                          String columnName,
                          QueryPlan.PredicateOperator operator,
                          DataBox value) throws QueryPlanException {
        super(OperatorType.SELECT, source);
        this.operator = operator;
        this.value = value;
        this.columnName = this.checkSchemaForColumn(source.getOutputSchema(), columnName);
        this.columnIndex = this.getOutputSchema().getFieldNames().indexOf(this.columnName);
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public RecordSchema computeSchema() throws QueryPlanException {
        return this.getSource().getOutputSchema();
    }

    public String str() {
        return "type: " + this.getType() +
                "\ncolumn: " + this.columnName +
                "\noperator: " + this.operator +
                "\nvalue_list: " + this.value;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    public TableStats estimateStats() throws QueryPlanException {
        TableStats stats = this.getSource().getStats();
        return stats.copyWithPredicate(this.columnIndex,
                this.operator,
                this.value);
    }

    public int estimateIOCost() throws QueryPlanException {
        return this.getSource().getIOCost();
    }

    public Iterator<SchemaRecord> iterator() throws QueryPlanException, DatabaseException {
        return new SelectIterator();
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class SelectIterator implements Iterator<SchemaRecord> {
        private final Iterator<SchemaRecord> sourceIterator;
        private final MarkerRecord markerRecord;
        private SchemaRecord nextRecord;

        public SelectIterator() throws QueryPlanException, DatabaseException {
            this.sourceIterator = SelectOperator.this.getSource().iterator();
            this.markerRecord = MarkerRecord.getMarker();
            this.nextRecord = null;
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
            while (this.sourceIterator.hasNext()) {
                SchemaRecord r = this.sourceIterator.next();
                if (r == this.markerRecord) {
                    this.nextRecord = r;
                    return true;
                }
                switch (SelectOperator.this.operator) {
                    case EQUALS:
                        if (r.getValues().get(SelectOperator.this.columnIndex).equals(value)) {
                            this.nextRecord = r;
                            return true;
                        }
                        break;
                    case NOT_EQUALS:
                        if (!r.getValues().get(SelectOperator.this.columnIndex).equals(value)) {
                            this.nextRecord = r;
                            return true;
                        }
                        break;
                    case LESS_THAN:
                        if (r.getValues().get(SelectOperator.this.columnIndex).compareTo(value) == -1) {
                            this.nextRecord = r;
                            return true;
                        }
                        break;
                    case LESS_THAN_EQUALS:
                        if (r.getValues().get(SelectOperator.this.columnIndex).compareTo(value) == -1) {
                            this.nextRecord = r;
                            return true;
                        } else if (r.getValues().get(SelectOperator.this.columnIndex).compareTo(value) == 0) {
                            this.nextRecord = r;
                            return true;
                        }
                        break;
                    case GREATER_THAN:
                        if (r.getValues().get(SelectOperator.this.columnIndex).compareTo(value) == 1) {
                            this.nextRecord = r;
                            return true;
                        }
                        break;
                    case GREATER_THAN_EQUALS:
                        if (r.getValues().get(SelectOperator.this.columnIndex).compareTo(value) == 1) {
                            this.nextRecord = r;
                            return true;
                        } else if (r.getValues().get(SelectOperator.this.columnIndex).compareTo(value) == 0) {
                            this.nextRecord = r;
                            return true;
                        }
                        break;
                    default:
                        break;
                }
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
