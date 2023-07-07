package durability.logging.LoggingStrategy;

import durability.ftmanager.FTManager;
import durability.recovery.RedoLogResult;
import durability.recovery.histroyviews.HistoryViews;
import durability.struct.Logging.LoggingEntry;
import storage.table.RecordSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface LoggingManager {
    void registerTable(RecordSchema recordSchema, String tableName);
    void addLogRecord(LoggingEntry logRecord);
    void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException;
    void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException;
    boolean inspectAbortView(long groupId, int threadId, long bid);
    int inspectAbortNumber(long groupId, int threadId);
    Object inspectDependencyView(long groupId, String table, String from, String to, long bid);
    HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId);
    HistoryViews getHistoryViews();
    void selectiveLoggingPartition(int partitionId);
}
