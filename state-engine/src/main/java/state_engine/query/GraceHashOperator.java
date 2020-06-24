package state_engine.query;

import state_engine.DatabaseException;
import state_engine.SimpleDatabase;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.DataBox;

import java.util.*;


public class GraceHashOperator extends JoinOperator {

    private int numBuffers;//?

    public GraceHashOperator(QueryOperator leftSource,
                             QueryOperator rightSource,
                             String leftColumnName,
                             String rightColumnName,
                             SimpleDatabase.Transaction transaction) throws QueryPlanException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.GRACEHASH);

//		this.numBuffers = transaction.getNumMemoryPages();
        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    public Iterator<SchemaRecord> iterator() throws QueryPlanException, DatabaseException {
        return new GraceHashIterator();
    }

    public int estimateIOCost() {
        /* TODO: Implement me! */

//		int io;
//		int r = GraceHashOperator.this.getLeftSource().getStats().getNumPages();
//		int s = GraceHashOperator.this.getRightSource().getStats().getNumPages();
//		io = (r + s) * 3;
//		return io;
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     */
    private class GraceHashIterator implements Iterator<SchemaRecord> {
        private Iterator<SchemaRecord> leftIterator;
        private Iterator<SchemaRecord> rightIterator;
        private SchemaRecord rightRecord;
        private SchemaRecord nextRecord;
        private String[] leftPartitions;
        private String[] rightPartitions;
        private int currentPartition;
        private Map<DataBox, ArrayList<SchemaRecord>> inMemoryHashTable;
        private int currIndexInList;
        private ArrayList<SchemaRecord> currList;
        private List<DataBox> rightRecordVals;

        public GraceHashIterator() throws QueryPlanException, DatabaseException {
            this.leftIterator = getLeftSource().iterator();
            this.rightIterator = getRightSource().iterator();
            leftPartitions = new String[numBuffers - 1];
            rightPartitions = new String[numBuffers - 1];
            this.inMemoryHashTable = new HashMap<>();
            currIndexInList = 0;
            String leftTableName;
            String rightTableName;
            for (int i = 0; i < numBuffers - 1; i++) {
                leftTableName = "Temp HashJoin Left Partition " + Integer.toString(i);
                rightTableName = "Temp HashJoin Right Partition " + Integer.toString(i);
                GraceHashOperator.this.createTempTable(getLeftSource().getOutputSchema(), leftTableName);
                GraceHashOperator.this.createTempTable(getRightSource().getOutputSchema(), rightTableName);
                leftPartitions[i] = leftTableName;
                rightPartitions[i] = rightTableName;
            }
            List<DataBox> values;
            DataBox val;
            int partition;
            while (this.leftIterator.hasNext()) {
                final SchemaRecord row = this.leftIterator.next();
                values = row.getValues();
                val = values.get(GraceHashOperator.this.getLeftColumnIndex());
                partition = val.hashCode() % (numBuffers - 1);
                GraceHashOperator.this.addRecord(leftPartitions[partition], row);
            }
            while (this.rightIterator.hasNext()) {
                final SchemaRecord row = this.rightIterator.next();
                values = row.getValues();
                val = values.get(GraceHashOperator.this.getRightColumnIndex());
                partition = val.hashCode() % (numBuffers - 1);
                GraceHashOperator.this.addRecord(rightPartitions[partition], row);
            }
            this.currentPartition = 0;
            leftIterator = GraceHashOperator.this.getTableIterator(leftPartitions[currentPartition]);
            while (leftIterator.hasNext()) {
                SchemaRecord currRecord = leftIterator.next();
                values = currRecord.getValues();
                val = values.get(GraceHashOperator.this.getLeftColumnIndex());
                if (this.inMemoryHashTable.containsKey(val)) {
                    this.inMemoryHashTable.get(val).add(currRecord);
                } else {
                    ArrayList<SchemaRecord> newList = new ArrayList<>();
                    newList.add(currRecord);
                    this.inMemoryHashTable.put(val, newList);
                }
            }
            this.nextRecord = null;
            this.rightIterator = GraceHashOperator.this.getTableIterator(rightPartitions[currentPartition]);
            if (this.rightIterator.hasNext()) {
                this.rightRecord = this.rightIterator.next();
                this.rightRecordVals = this.rightRecord.getValues();
                this.currList = this.inMemoryHashTable.get(this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex()));
            }

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
                while (this.currList == null && this.rightIterator.hasNext()) {
                    this.rightRecord = this.rightIterator.next();
                    this.rightRecordVals = this.rightRecord.getValues();
                    this.currIndexInList = 0;
                    this.currList = this.inMemoryHashTable.get(this.rightRecord.getValues().get(GraceHashOperator.this.getRightColumnIndex()));
                }
                if (this.currList != null) {
                    if (this.currIndexInList < this.currList.size()) {
                        List<DataBox> leftValues = new ArrayList<>(this.currList.get(this.currIndexInList).getValues());
                        leftValues.addAll(rightRecordVals);
                        this.nextRecord = new SchemaRecord(leftValues);
                        this.currIndexInList++;
                        return true;
                    } else {
                        this.currList = null;
                    }
                } else {
                    if (this.currentPartition < numBuffers - 2) {
                        this.currentPartition++;
                        try {
                            this.rightIterator = GraceHashOperator.this.getTableIterator(rightPartitions[currentPartition]);
                            this.leftIterator = GraceHashOperator.this.getTableIterator(leftPartitions[currentPartition]);
                        } catch (DatabaseException d) {
                            return false;
                        }
                        this.inMemoryHashTable = new HashMap<>();
                        while (leftIterator.hasNext()) {
                            SchemaRecord currRecord = leftIterator.next();
                            List<DataBox> values = currRecord.getValues();
                            DataBox val = values.get(GraceHashOperator.this.getLeftColumnIndex());
                            if (this.inMemoryHashTable.containsKey(val)) {
                                this.inMemoryHashTable.get(val).add(currRecord);
                            } else {
                                ArrayList<SchemaRecord> newList = new ArrayList<>();
                                newList.add(currRecord);
                                this.inMemoryHashTable.put(val, newList);
                            }
                        }
                        this.currList = null;
                    } else {
                        return false;
                    }
                }
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