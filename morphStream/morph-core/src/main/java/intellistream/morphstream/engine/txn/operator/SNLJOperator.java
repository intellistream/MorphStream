package intellistream.morphstream.engine.txn.operator;

import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.db.SimpleDatabase;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SNLJOperator extends JoinOperator {
    private final QueryOperator leftSource;
    private final QueryOperator rightSource;
    private final int leftColumnIndex;
    private final int rightColumnIndex;
    private final String leftColumnName;
    private final String rightColumnName;
    private final SimpleDatabase.Transaction transaction;

    public SNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        SimpleDatabase.Transaction transaction) throws QueryPlanException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.SNLJ);
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
        this.leftSource = getLeftSource();
        this.rightSource = getRightSource();
        this.leftColumnIndex = getLeftColumnIndex();
        this.rightColumnIndex = getRightColumnIndex();
        this.leftColumnName = getLeftColumnName();
        this.rightColumnName = getRightColumnName();
        this.transaction = getTransaction();
    }

    public Iterator<SchemaRecord> iterator() throws QueryPlanException, DatabaseException {
        return new SNLJIterator();
    }

    public int estimateIOCost() {
        /* TODO: Implement me! */
//
//		int io;
//		int pR = SNLJOperator.this.getLeftSource().getStats().getNumRecords();
//		int r = SNLJOperator.this.getLeftSource().getStats().getNumPages();
//		int s = SNLJOperator.this.getRightSource().getStats().getNumPages();
//		io = pR * s + r;
//		return io;
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class SNLJIterator implements Iterator<SchemaRecord> {
        private final Iterator<SchemaRecord> leftIterator;
        private Iterator<SchemaRecord> rightIterator;
        private SchemaRecord leftRecord;
        private SchemaRecord nextRecord;

        public SNLJIterator() throws QueryPlanException, DatabaseException {
            this.leftIterator = SNLJOperator.this.getLeftSource().iterator();
            this.rightIterator = null;
            this.leftRecord = null;
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
            while (true) {
                if (this.leftRecord == null) {
                    if (this.leftIterator.hasNext()) {
                        this.leftRecord = this.leftIterator.next();
                        try {
                            this.rightIterator = SNLJOperator.this.getRightSource().iterator();
                        } catch (QueryPlanException | DatabaseException q) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                while (this.rightIterator.hasNext()) {
                    SchemaRecord rightRecord = this.rightIterator.next();
                    DataBox leftJoinValue = this.leftRecord.getValues().get(SNLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(SNLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new SchemaRecord(leftValues);
                        return true;
                    }
                }
                this.leftRecord = null;
            }
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
