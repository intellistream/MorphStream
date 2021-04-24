package state_engine.query;
import state_engine.db.DatabaseException;
import state_engine.storage.MarkerRecord;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.DataBox;
import state_engine.storage.datatype.FloatDataBox;
import state_engine.storage.datatype.IntDataBox;
import state_engine.storage.table.RecordSchema;
import state_engine.storage.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
public class ProjectOperator extends QueryOperator {
    private List<String> columns;
    private List<Integer> indices;
    private boolean hasCount;
    private int averageColumnIndex;
    private int sumColumnIndex;
    private boolean hasAggregate = false;
    private int countValue;
    private double sumValue;
    private double averageSumValue;
    private int averageCountValue;
    private String sumColumn;
    private String averageColumn;
    private boolean sumIsFloat;
    /**
     * Creates a new ProjectOperator that reads tuples from source and filters out columns. Optionally
     * computers an aggregate if it is specified.
     *
     * @param source
     * @param columns
     * @param count
     * @param averageColumn
     * @param sumColumn
     * @throws QueryPlanException
     */
    public ProjectOperator(QueryOperator source,
                           List<String> columns,
                           boolean count,
                           String averageColumn,
                           String sumColumn) throws QueryPlanException {
        super(OperatorType.PROJECT);
        this.columns = columns;
        this.indices = new ArrayList<>();
        this.countValue = 0;
        this.sumValue = 0;
        this.averageCountValue = 0;
        this.averageSumValue = 0;
        this.averageColumnIndex = -1;
        this.sumColumnIndex = -1;
        this.sumColumn = sumColumn;
        this.averageColumn = averageColumn;
        this.hasCount = count;
        this.hasAggregate = this.hasCount || averageColumn != null || sumColumn != null;
        // NOTE: Don't need to explicitly set the output schema because setting the source recomputes
        // the schema for the query optimization case.
        this.setSource(source);
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }
    protected RecordSchema computeSchema() throws QueryPlanException {
        // check to make sure that the source operator is giving us columns that we project
        RecordSchema sourceSchema = this.getSource().getOutputSchema();
        List<String> sourceColumnNames = new ArrayList<>(sourceSchema.getFieldNames());
        List<DataBox> sourceColumnTypes = new ArrayList<>(sourceSchema.getFieldTypes());
        List<DataBox> columnTypes = new ArrayList<>();
        for (String columnName : this.columns) {
            columnName = this.checkSchemaForColumn(sourceSchema, columnName);
            int sourceColumnIndex = sourceColumnNames.indexOf(columnName);
            columnTypes.add(sourceColumnTypes.get(sourceColumnIndex));
            this.indices.add(sourceColumnIndex);
        }
        if (this.sumColumn != null) {
            this.sumColumn = this.checkSchemaForColumn(sourceSchema, this.sumColumn);
            this.sumColumnIndex = sourceColumnNames.indexOf(this.sumColumn);
            if (!(sourceColumnTypes.get(this.sumColumnIndex) instanceof IntDataBox) &&
                    !(sourceColumnTypes.get(this.sumColumnIndex) instanceof FloatDataBox)) {
                throw new QueryPlanException("Cannot compute sum over a non-integer column: " + this.sumColumn + ".");
            }
        }
        if (this.averageColumn != null) {
            this.averageColumn = this.checkSchemaForColumn(sourceSchema, this.averageColumn);
            this.averageColumnIndex = sourceColumnNames.indexOf(this.averageColumn);
            if (!(sourceColumnTypes.get(this.averageColumnIndex) instanceof IntDataBox) &&
                    !(sourceColumnTypes.get(this.sumColumnIndex) instanceof FloatDataBox)) {
                throw new QueryPlanException("Cannot compute sum over a non-integer column: " + this.averageColumn + ".");
            }
        }
        // make sure we add the correct columns to the output schema if we have aggregates in the
        // projection
        if (this.hasAggregate) {
            if (this.hasCount) {
                this.columns.add("countAgg");
                columnTypes.add(new IntDataBox());
            }
            if (this.sumColumn != null) {
                this.columns.add("sumAgg");
                if (sourceColumnTypes.get(this.sumColumnIndex) instanceof IntDataBox) {
                    columnTypes.add(new IntDataBox());
                    this.sumIsFloat = false;
                } else {
                    columnTypes.add(new FloatDataBox());
                    this.sumIsFloat = true;
                }
            }
            if (this.averageColumn != null) {
                this.columns.add("averageAgg");
                columnTypes.add(new FloatDataBox());
            }
        }
        return new RecordSchema(this.columns, columnTypes);
    }
    public Iterator<SchemaRecord> iterator() throws QueryPlanException, DatabaseException {
        return new ProjectIterator();
    }
    private void addToCount() {
        this.countValue++;
    }
    private int getAndResetCount() {
        int result = this.countValue;
        this.countValue = 0;
        return result;
    }
    private void addToSum(SchemaRecord record) {
        if (this.sumIsFloat) {
            this.sumValue += record.getValues().get(this.sumColumnIndex).getFloat();
        } else {
            this.sumValue += record.getValues().get(this.sumColumnIndex).getInt();
        }
    }
    private double getAndResetSum() {
        double result = this.sumValue;
        this.sumValue = 0;
        return result;
    }
    private void addToAverage(SchemaRecord record) {
        this.averageCountValue++;
        this.averageSumValue += record.getValues().get(this.averageColumnIndex).getInt();
    }
    private double getAndResetAverage() {
        if (this.averageCountValue == 0) {
            return 0f;
        }
        double result = this.averageSumValue / this.averageCountValue;
        this.averageSumValue = 0;
        this.averageCountValue = 0;
        return result;
    }
    public String str() {
        return "type: " + this.getType() +
                "\ncolumns: " + this.columns;
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
    private class ProjectIterator implements Iterator<SchemaRecord> {
        private Iterator<SchemaRecord> sourceIterator;
        private MarkerRecord markerRecord;
        private SchemaRecord nextRecord;
        private boolean prevWasMarker;
        private List<DataBox> baseValues;
        public ProjectIterator() throws QueryPlanException, DatabaseException {
            this.sourceIterator = ProjectOperator.this.getSource().iterator();
            this.markerRecord = MarkerRecord.getMarker();
            this.nextRecord = null;
            this.prevWasMarker = true;
            this.baseValues = new ArrayList<>();
        }
        /**
         * Checks if there are more d_record(s) to yield
         *
         * @return true if this iterator has another d_record to yield, otherwise false
         */
        public boolean hasNext() {
            return this.sourceIterator.hasNext();
        }
        /**
         * Yields the next d_record of this iterator.
         *
         * @return the next SchemaRecord
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public SchemaRecord next() {
            if (this.hasNext()) {
                if (ProjectOperator.this.hasAggregate) {
                    while (this.sourceIterator.hasNext()) {
                        SchemaRecord r = this.sourceIterator.next();
                        List<DataBox> recordValues = r.getValues();
                        // if the d_record is a MarkerRecord, that means we reached the end of a group... we reset
                        // the aggregates and add the appropriate new d_record to the new Records
                        if (r == this.markerRecord) {
                            if (ProjectOperator.this.hasCount) {
                                int count = ProjectOperator.this.getAndResetCount();
                                this.baseValues.add(new IntDataBox(count));
                            }
                            if (ProjectOperator.this.sumColumnIndex != -1) {
                                double sum = ProjectOperator.this.getAndResetSum();
                                if (ProjectOperator.this.sumIsFloat) {
                                    this.baseValues.add(new FloatDataBox((float) sum));
                                } else {
                                    this.baseValues.add(new IntDataBox((int) sum));
                                }
                            }
                            if (ProjectOperator.this.averageColumnIndex != -1) {
                                double average = (float) ProjectOperator.this.getAndResetAverage();
                                this.baseValues.add(new FloatDataBox((float) average));
                            }
                            // d_record that we just saw a marker d_record
                            this.prevWasMarker = true;
                            return new SchemaRecord(this.baseValues);
                        } else {
                            // if the previous d_record was a marker (or for the first d_record) we have to get the relevant
                            // fields out of the d_record
                            if (this.prevWasMarker) {
                                this.baseValues = new ArrayList<>();
                                for (int index : ProjectOperator.this.indices) {
                                    this.baseValues.add(recordValues.get(index));
                                }
                                this.prevWasMarker = false;
                            }
                            if (ProjectOperator.this.hasCount) {
                                ProjectOperator.this.addToCount();
                            }
                            if (ProjectOperator.this.sumColumnIndex != -1) {
                                ProjectOperator.this.addToSum(r);
                            }
                            if (ProjectOperator.this.averageColumnIndex != -1) {
                                ProjectOperator.this.addToAverage(r);
                            }
                        }
                    }
                    // at the very end, we need to make sure we add all the aggregated records to the result
                    // either because there was no group by or to add the last group we saw
                    if (ProjectOperator.this.hasCount) {
                        int count = ProjectOperator.this.getAndResetCount();
                        this.baseValues.add(new IntDataBox(count));
                    }
                    if (ProjectOperator.this.sumColumnIndex != -1) {
                        double sum = ProjectOperator.this.getAndResetSum();
                        if (ProjectOperator.this.sumIsFloat) {
                            this.baseValues.add(new FloatDataBox((float) sum));
                        } else {
                            this.baseValues.add(new IntDataBox((int) sum));
                        }
                    }
                    if (ProjectOperator.this.averageColumnIndex != -1) {
                        double average = ProjectOperator.this.getAndResetAverage();
                        this.baseValues.add(new FloatDataBox((float) average));
                    }
                    return new SchemaRecord(this.baseValues);
                } else {
                    SchemaRecord r = this.sourceIterator.next();
                    List<DataBox> recordValues = r.getValues();
                    List<DataBox> newValues = new ArrayList<>();
                    // if there is a marker d_record (in the case we're projecting from a group by), we simply
                    // leave the marker records in
                    if (r == this.markerRecord) {
                        return markerRecord;
                    } else {
                        for (int index : ProjectOperator.this.indices) {
                            newValues.add(recordValues.get(index));
                        }
                        return new SchemaRecord(newValues);
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
