package engine.txn.durability.recovery.lsnvector;

import engine.txn.durability.struct.Logging.LVCLog;

import java.util.concurrent.ConcurrentSkipListSet;

public class CSContext {
    public int threadId;
    public ConcurrentSkipListSet<LVCLog> tasks = new ConcurrentSkipListSet<>();
    public LVCLog readyTask;
    public int totalTaskCount;
    public int scheduledTaskCount;

    public CSContext(int threadId) {
        this.threadId = threadId;
    }
    public void reset() {
        tasks.clear();
        totalTaskCount = 0;
        scheduledTaskCount = 0;
    }
    public void addTask(LVCLog task) {
        tasks.add(task);
    }

    public boolean isFinished() {
        assert scheduledTaskCount <= totalTaskCount;
        return scheduledTaskCount == totalTaskCount;
    }
}
