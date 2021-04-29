package operator;
import db.DatabaseException;
import db.SimpleDatabase;
import storage.SchemaRecord;
import storage.table.RecordSchema;
import storage.table.stats.TableStats;

import java.util.Iterator;
public class SequentialScanOperator extends QueryOperator {
    private SimpleDatabase.Transaction transaction;
    private String tableName;
    /**
     * Creates a new SequentialScanOperator that provides an iterator on all tuples in a table.
     * <p>
     * NOTE: Sequential scans don't take a source operator because they must always be at the bottom
     * of the DAG.
     *
     * @param transaction
     * @param tableName
     * @throws QueryPlanException
     * @throws DatabaseException
     */
    public SequentialScanOperator(SimpleDatabase.Transaction transaction,
                                  String tableName) throws QueryPlanException {
        super(OperatorType.SEQSCAN);
        this.transaction = transaction;
        this.tableName = tableName;
        this.setOutputSchema(this.computeSchema());
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }
    public String getTableName() {
        return this.tableName;
    }
    public Iterator<SchemaRecord> iterator() throws DatabaseException {
        return this.transaction.getRecordIterator(tableName);
    }
    public RecordSchema computeSchema() throws QueryPlanException {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }
    public String str() {
        return "type: " + this.getType() +
                "\ntable: " + this.tableName;
    }
    /**
     * Estimates the table statistics for the result of executing this query operator.
     *
     * @return estimated TableStats
     */
    public TableStats estimateStats() throws QueryPlanException {
        try {
            return this.transaction.getStats(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }
    public int estimateIOCost() {
//		try {
//			return this.transaction.getNumDataPages(this.tableName);
//		} catch (DatabaseException de) {
//			throw new QueryPlanException(de);
//		}
        return 0;
    }
}
