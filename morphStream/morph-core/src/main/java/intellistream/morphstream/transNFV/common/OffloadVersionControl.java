package intellistream.morphstream.transNFV.common;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.transNFV.vnf.UDF;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Used in Offloading-MVCC, each OffloadVersionControl manages MVCC for a single key in the table
 * */

public class OffloadVersionControl {
    private final ReentrantLock writeLock = new ReentrantLock();
    private final Condition writeCondition = writeLock.newCondition();
    private final TableRecord tableRecord;
    private final PriorityQueue<Operation> writeRequestQueue;
    private long lwm = Long.MAX_VALUE;

    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private long usefulStartTime = 0;
    private long usefulTimePerOperation = 0;

    public OffloadVersionControl(TableRecord tableRecord) {
        this.tableRecord = tableRecord;
        writeRequestQueue = new PriorityQueue<>(Comparator.comparingLong(Operation::getTimestamp));
    }

    public void lock() {
        writeLock.lock();
    }

    public void unlock() {
        writeLock.unlock();
    }

    public void enqueueWriteRequest(int key, int value, long timestamp, boolean isWrite) {
        writeRequestQueue.add(new Operation(key, value, timestamp, isWrite));
        lwm = Math.min(lwm, timestamp);
    }

    public boolean canGrantWrite(long timestamp) {
        assert writeRequestQueue.peek() != null;
        return (timestamp == writeRequestQueue.peek().getTimestamp());
    }

    public boolean canGrantRead(long timestamp) {
        return timestamp < lwm;
    }

    public void awaitForWrite() throws InterruptedException {
        writeCondition.await();
    }

    public void signalAll() {
        writeCondition.signalAll();
    }

    public void signalNext() {
        writeCondition.signal();
    }

    public long readVersion(long timestamp) {
        REC_usefulStartTime();
        tableRecord.content_.readPreValues(timestamp).getValues().get(1).getInt();
        REC_usefulEndTime();
        return usefulTimePerOperation;
    }

    public long writeVersion(VNFRequest request) {
        int value = request.getValue();
        long timestamp = request.getCreateTime();

        REC_usefulStartTime();
        SchemaRecord tempo_record = new SchemaRecord(tableRecord.content_.readPreValues(timestamp));
        tempo_record.getValues().get(1).setInt(value);
        tableRecord.content_.updateMultiValues(timestamp, timestamp, false, tempo_record);
        UDF.executeUDF(request);
        REC_usefulEndTime();

        writeRequestQueue.poll();
        lwm = writeRequestQueue.isEmpty() ? Long.MAX_VALUE : writeRequestQueue.peek().getTimestamp();

        return usefulTimePerOperation;
    }

    private void REC_usefulStartTime() {
        if (enableTimeBreakdown) {
            usefulStartTime = System.nanoTime();
        }
    }

    private void REC_usefulEndTime() {
        if (enableTimeBreakdown) {
            usefulTimePerOperation = System.nanoTime() - usefulStartTime;
        }
    }
}
