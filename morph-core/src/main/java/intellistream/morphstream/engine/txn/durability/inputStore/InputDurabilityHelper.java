package intellistream.morphstream.engine.txn.durability.inputStore;

import intellistream.morphstream.engine.txn.durability.recovery.histroyviews.HistoryViews;
import intellistream.morphstream.util.FaultToleranceConstants;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ExecutionException;

public abstract class InputDurabilityHelper {
    public int partitionOffset;
    public int tthread;
    public int taskId;
    public int ftOption;
    public FaultToleranceConstants.CompressionType encodingType;
    public boolean isCompression = true;
    public HistoryViews historyViews = new HistoryViews();

    public abstract void reloadInput(File inputFile, Queue<Object> lostEvents, long redoOffset, long startOffset, int interval) throws IOException, ExecutionException, InterruptedException;

    public abstract void storeInput(Object[] myevents, long currentOffset, int interval, String inputStoreCurrentPath) throws IOException, ExecutionException, InterruptedException;
}
