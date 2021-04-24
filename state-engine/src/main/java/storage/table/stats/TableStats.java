package storage.table.stats;
import query.QueryPlan;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import storage.table.RecordSchema;

import java.util.ArrayList;
import java.util.List;
/**
 * A wrapper class to represent the statistics for a partition table.
 * Note that statistics are an estimate over the data in a table.
 * An instance of TableStats contains a list of Histograms that each
 * provide statistics about individual columns of a schema.
 * <p>
 * YOU SHOULD NOT NEED TO CHANGE ANY OF THE CODE IN THIS CLASS.
 */
public class TableStats {
    private boolean estimate;
    private int numRecords;
    private RecordSchema tableSchema;
    private List<Histogram> histograms;
    /**
     * Creates a new TableStats with a given RecordSchema.
     *
     * @param tableSchema the schema instance associated with the target table
     */
    public TableStats(RecordSchema tableSchema) {
        this.estimate = false;
        this.numRecords = 0;
        this.tableSchema = tableSchema;
        this.histograms = new ArrayList<>();
        for (DataBox dataType : tableSchema.getFieldTypes()) {
            switch (dataType.type()) {
                case INT:
                    this.histograms.add(new IntHistogram());
                    break;
                case BOOL:
                    this.histograms.add(new BoolHistogram());
                    break;
                case STRING:
                    this.histograms.add(new StringHistogram());
                    break;
                default:
                    this.histograms.add(new ObjectHistogram());//use generic histogram for others.
                    break;
            }
        }
    }
    /**
     * Creates a new TableStats with a schema, a list of histograms,
     * and an estimate number of records.
     *
     * @param tableSchema the schema instance associated with the target table
     * @param histograms  a list of histograms associated with the fields in tableSchema
     * @param numRecords  the estimate number of records the target table contains
     */
    public TableStats(RecordSchema tableSchema, List<Histogram> histograms, int numRecords) {
        this.estimate = true;
        this.numRecords = numRecords;
        this.tableSchema = tableSchema;
        this.histograms = histograms;
    }
    /**
     * Adds the stats for a new d_record.
     *
     * @param record the new d_record
     */
    public void addRecord(SchemaRecord record) {
        this.numRecords++;
        int count = 0;
        for (DataBox value : record.getValues()) {
            switch (value.type()) {
                case INT:
                    this.histograms.get(count).add(value.getInt());
                    break;
                case STRING:
                    this.histograms.get(count).add(value.getString());
                    break;
                case BOOL:
                    this.histograms.get(count).add(value.getBool());
                    break;
                case FLOAT:
                    this.histograms.get(count).add(value.getFloat());
                    break;
                default:
                    break;
            }
            count++;
        }
    }
    /**
     * Remove the stats for an existing d_record.
     *
     * @param record the new d_record
     */
    public void removeRecord(SchemaRecord record) {
        this.numRecords--;
        int count = 0;
        for (DataBox value : record.getValues()) {
            switch (value.type()) {
                case INT:
                    this.histograms.get(count).removeValue(value.getInt());
                    break;
                case STRING:
                    this.histograms.get(count).removeValue(value.getString());
                    break;
                case BOOL:
                    this.histograms.get(count).removeValue(value.getBool());
                    break;
                case FLOAT:
                    this.histograms.get(count).removeValue(value.getFloat());
                    break;
                default:
                    break;
            }
            count++;
        }
    }
    /**
     * Clean the table states
     */
    public void clean() {
        this.numRecords = 0;
        this.histograms = new ArrayList<>();
        for (DataBox dataType : tableSchema.getFieldTypes()) {
            switch (dataType.type()) {
                case INT:
                    this.histograms.add(new IntHistogram());
                    break;
                case BOOL:
                    this.histograms.add(new BoolHistogram());
                    break;
                case STRING:
                    this.histograms.add(new StringHistogram());
                    break;
                default:
                    this.histograms.add(new ObjectHistogram());//use generic histogram for others.
                    break;
            }
        }
    }
    /**
     * Gets the number of records the target table contains.
     *
     * @return number of records
     */
    public int getNumRecords() {
        return this.numRecords;
    }
    /**
     * Gets the estimate reduction factor a predicate
     * and value_list would result in over a certain column.
     *
     * @param index     the index of column in question
     * @param predicate the predicate to compute reduction factor with
     * @param value     the value_list to compare against
     * @return estimate reduction factor
     */
    public float getReductionFactor(int index,
                                    QueryPlan.PredicateOperator predicate,
                                    DataBox value) {
        return this.getHistogram(index).computeReductionFactor(predicate, value);
    }
    /**
     * Creates a new TableStats which is the statistics for the table
     * that results from the given predicate and value_list applied to
     * the target table associated with this TableStats.
     *
     * @param index     the index of column in question
     * @param predicate the predicate to compute reduction factor with
     * @param value     the value_list to compare against
     * @return new TableStats based off of this and params
     */
    public TableStats copyWithPredicate(int index,
                                        QueryPlan.PredicateOperator predicate,
                                        DataBox value) {
        List<Histogram> copyHistograms = new ArrayList<>();
        Histogram predHistogram = this.histograms.get(index);
        float reductionFactor = predHistogram.computeReductionFactor(predicate, value);
        for (int i = 0; i < this.histograms.size(); i++) {
            if (i == index) {
                copyHistograms.add(predHistogram.copyWithPredicate(predicate, value));
            } else {
                Histogram histogram = this.histograms.get(i);
                copyHistograms.add(histogram.copyWithReduction(reductionFactor));
            }
        }
        int numRecords = (int) (this.numRecords * reductionFactor);
        return new TableStats(this.tableSchema, copyHistograms, numRecords);
    }
    /**
     * Creates a new TableStats which is the statistics for the table
     * that results from this TableStats joined with the given TableStats.
     *
     * @param leftIndex  the index of the join column for this
     * @param rightStats the TableStats of the right table to be joined
     * @param leftIndex  the index of the join column for the right table
     * @return new TableStats based off of this and params
     */
    public TableStats copyWithJoin(int leftIndex,
                                   TableStats rightStats,
                                   int rightIndex) {
        // Assume `this` is the `TableStats` instance for the left relation.
        RecordSchema rightSchema = rightStats.getSchema();
        List<Histogram> rightHistograms = rightStats.getHistograms();
        List<String> rightFieldNames = rightSchema.getFieldNames();
        List<DataBox> rightDataBoxs = rightSchema.getFieldTypes();
        List<String> copyFields = new ArrayList<>();
        for (String leftFieldName : this.tableSchema.getFieldNames()) {
            copyFields.add(leftFieldName);
        }
        for (String rightFieldName : rightFieldNames) {
            copyFields.add(rightFieldName);
        }
        List<DataBox> copyDataBoxs = new ArrayList<>();
        for (DataBox leftDataBox : this.tableSchema.getFieldTypes()) {
            copyDataBoxs.add(leftDataBox);
        }
        for (DataBox rightDataBox : rightDataBoxs) {
            copyDataBoxs.add(rightDataBox);
        }
        RecordSchema copySchema = new RecordSchema(copyFields, copyDataBoxs);
        int inputSize = this.numRecords * rightStats.getNumRecords();
        int leftNumDistinct = this.getNumDistinct(leftIndex);
        int rightNumDistinct = rightStats.getNumDistinct(rightIndex);
        float reductionFactor = 1.0f / Math.max(leftNumDistinct, rightNumDistinct);
        List<Histogram> copyHistograms = new ArrayList<>();
        int leftNumRecords = this.numRecords;
        int rightNumRecords = rightStats.getNumRecords();
        float leftReductionFactor = ((float) inputSize / leftNumRecords) * reductionFactor;
        float rightReductionFactor = ((float) inputSize / rightNumRecords) * reductionFactor;
        float joinReductionFactor = leftReductionFactor;
        Histogram joinHistogram = this.histograms.get(leftIndex);
        for (int i = 0; i < this.histograms.size(); i++) {
            Histogram leftHistogram = this.histograms.get(i);
            if (i == leftIndex) {
                copyHistograms.add(joinHistogram.copyWithReduction(joinReductionFactor));
            } else {
                copyHistograms.add(leftHistogram.copyWithReduction(leftReductionFactor));
            }
        }
        for (int i = 0; i < rightHistograms.size(); i++) {
            Histogram rightHistogram = rightHistograms.get(i);
            if (i == rightIndex) {
                copyHistograms.add(joinHistogram.copyWithReduction(joinReductionFactor));
            } else {
                copyHistograms.add(rightHistogram.copyWithReduction(rightReductionFactor));
            }
        }
        int outputSize = (int) (inputSize * reductionFactor);
        return new TableStats(copySchema, copyHistograms, outputSize);
    }
    /**
     * Gets the histogram for a certain column.
     *
     * @param index the index of column in question
     * @return the histogram corresponding to index
     */
    public Histogram getHistogram(int index) {
        return this.histograms.get(index);
    }
    /**
     * Gets the histograms of this TableStatistics.
     *
     * @return list of histograms
     */
    public List<Histogram> getHistograms() {
        return this.histograms;
    }
    /**
     * Gets the schema of this TableStatistics.
     *
     * @return schema of this TableStats
     */
    public RecordSchema getSchema() {
        return this.tableSchema;
    }
    /**
     * Gets the estimate number of distinct entries for a certain column.
     *
     * @param index the index of column in question
     * @return estimate number of distinct entries
     */
    public int getNumDistinct(int index) {
        return this.getHistogram(index).getNumDistinct();
    }
}