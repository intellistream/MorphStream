package state_engine.query;

import state_engine.DatabaseException;
import state_engine.SimpleDatabase;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.DataBox;
import state_engine.storage.table.RecordSchema;
import state_engine.storage.table.RowID;
import state_engine.storage.table.stats.TableStats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class JoinOperator extends QueryOperator {

    private JoinType joinType;
    private QueryOperator leftSource;
    private QueryOperator rightSource;
    private int leftColumnIndex;
    private int rightColumnIndex;
    private String leftColumnName;
    private String rightColumnName;
    private SimpleDatabase.Transaction transaction;

    /**
     * Create a join operator that pulls tuples from leftSource and rightSource. Returns tuples for which
     * leftColumnName and rightColumnName are equal.
     *
     * @param leftSource      the left source operator
     * @param rightSource     the right source operator
     * @param leftColumnName  the column to join on from leftSource
     * @param rightColumnName the column to join on from rightSource
     * @throws QueryPlanException
     */
    public JoinOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        SimpleDatabase.Transaction transaction,
                        JoinType joinType) throws QueryPlanException {
        super(OperatorType.JOIN);
        this.joinType = joinType;
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.leftColumnName = leftColumnName;
        this.rightColumnName = rightColumnName;
        this.setOutputSchema(this.computeSchema());
        this.transaction = transaction;
    }

    public abstract Iterator<SchemaRecord> iterator() throws QueryPlanException, DatabaseException;

    @Override
    public QueryOperator getSource() throws QueryPlanException {
        throw new QueryPlanException("There is no partition source for join operators. Please use " +
                "getRightSource and getLeftSource and the corresponding set methods.");
    }

    public QueryOperator getLeftSource() {
        return this.leftSource;
    }

    public void setLeftSource(QueryOperator leftSource) {
        this.leftSource = leftSource;
    }

    public QueryOperator getRightSource() {
        return this.rightSource;
    }

    public void setRightSource(QueryOperator rightSource) {
        this.rightSource = rightSource;
    }

    public RecordSchema computeSchema() throws QueryPlanException {
        RecordSchema leftSchema = this.leftSource.getOutputSchema();
        RecordSchema rightSchema = this.rightSource.getOutputSchema();
        List<String> leftSchemaNames = new ArrayList<>(leftSchema.getFieldNames());
        List<String> rightSchemaNames = new ArrayList<>(rightSchema.getFieldNames());
        this.leftColumnName = this.checkSchemaForColumn(leftSchema, this.leftColumnName);
        this.leftColumnIndex = leftSchemaNames.indexOf(leftColumnName);
        this.rightColumnName = this.checkSchemaForColumn(rightSchema, this.rightColumnName);
        this.rightColumnIndex = rightSchemaNames.indexOf(rightColumnName);
        List<DataBox> leftSchemaTypes = new ArrayList<>(leftSchema.getFieldTypes());
        List<DataBox> rightSchemaTypes = new ArrayList<>(rightSchema.getFieldTypes());
        if (!leftSchemaTypes.get(this.leftColumnIndex).getClass().equals(rightSchemaTypes.get(
                this.rightColumnIndex).getClass())) {
            throw new QueryPlanException("Mismatched types of columns " + leftColumnName + " and "
                    + rightColumnName + ".");
        }
        leftSchemaNames.addAll(rightSchemaNames);
        leftSchemaTypes.addAll(rightSchemaTypes);
        return new RecordSchema(leftSchemaNames, leftSchemaTypes);
    }

    public String str() {
        return "type: " + this.joinType +
                "\nleftColumn: " + this.leftColumnName +
                "\nrightColumn: " + this.rightColumnName;
    }

    @Override
    public String toString() {
        String r = this.str();
        if (this.leftSource != null) {
            r += "\n" + ("(left)\n" + this.leftSource.toString()).replaceAll("(?m)^", "\t");
        }
        if (this.rightSource != null) {
            if (this.leftSource != null) {
                r += "\n";
            }
            r += "\n" + ("(right)\n" + this.rightSource.toString()).replaceAll("(?m)^", "\t");
        }
        return r;
    }

    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    public TableStats estimateStats() {
        TableStats leftStats = this.leftSource.getStats();
        TableStats rightStats = this.rightSource.getStats();

        return leftStats.copyWithJoin(this.leftColumnIndex,
                rightStats,
                this.rightColumnIndex);
    }

    public abstract int estimateIOCost() throws QueryPlanException;

    public RecordSchema getSchema(String tableName) throws DatabaseException {
        return this.transaction.getSchema(tableName);
    }


    public int getEntrySize(String tableName) throws DatabaseException {
        return this.transaction.getEntrySize(tableName);
    }


    public String getLeftColumnName() {
        return this.leftColumnName;
    }

    public String getRightColumnName() {
        return this.rightColumnName;
    }

    public SimpleDatabase.Transaction getTransaction() {
        return this.transaction;
    }

    public int getLeftColumnIndex() {
        return this.leftColumnIndex;
    }

    public int getRightColumnIndex() {
        return this.rightColumnIndex;
    }

    public Iterator<SchemaRecord> getTableIterator(String tableName) throws DatabaseException {
        return this.transaction.getRecordIterator(tableName);
    }

    public void createTempTable(RecordSchema schema, String tableName) throws DatabaseException {
        this.transaction.createTempTable(schema, tableName);
    }

    public RowID addRecord(String tableName, SchemaRecord row) throws DatabaseException {
        return this.transaction.addRecord(tableName, row);
    }

    public JoinType getJoinType() {
        return this.joinType;
    }

    public enum JoinType {
        SNLJ,
        PNLJ,
        BNLJ,
        GRACEHASH,
        SORTMERGE
    }
}
