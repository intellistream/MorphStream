package intellistream.morphstream.transNFV.data;

import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReentrantLock;

public class VersionControl {
    private final ReentrantLock lock = new ReentrantLock();
    private final TableRecord tableRecord;
    private final PriorityQueue<Operation> writeQueue;
    private long lwm = Long.MAX_VALUE;
    private boolean writing = false;

    public VersionControl(TableRecord tableRecord) {
        this.tableRecord = tableRecord;
        writeQueue = new PriorityQueue<>(Comparator.comparingLong(Operation::getTimestamp));
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public boolean isWriting() {
        return writing;
    }

    public void enqueueWriteRequest(int key, int value, long timestamp, boolean isWrite) {
        writeQueue.add(new Operation(key, value, timestamp, isWrite));
        lwm = Math.min(lwm, timestamp);
    }

    public boolean canGrantWrite(long timestamp) {
        return timestamp == writeQueue.peek().getTimestamp();
    }

    public boolean canGrantRead(long timestamp) {
        return timestamp < lwm;
    }

    public void commitWrite(long timestamp, int value) {
        SchemaRecord tempo_record = new SchemaRecord(tableRecord.content_.readPreValues(timestamp));
        tempo_record.getValues().get(1).setInt(value);
        tableRecord.content_.updateMultiValues(timestamp, timestamp, false, tempo_record);

        writeQueue.poll();
        lwm = writeQueue.isEmpty() ? Long.MAX_VALUE : writeQueue.peek().getTimestamp();
        writing = false;
    }

    public void awaitWrite() throws InterruptedException {
        lock.newCondition().await();
    }

    public void signalAll() {
        lock.newCondition().signalAll();
    }

    public int getVersion(long timestamp) {
        return tableRecord.content_.readPreValues(timestamp).getValues().get(1).getInt();
    }
}
