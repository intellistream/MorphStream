package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.db.storage.StorageManager;
import intellistream.morphstream.engine.db.storage.impl.RemoteStorageManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.scheduler.collector.Collector;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.ds.DSSchedule;
import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.OGNSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.OGNSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.nonstructured.TStreamScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGBFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGBFSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGDFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.og.structured.OGDFSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.nonstructured.OPNSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.nonstructured.OPNSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPBFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPBFSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPDFSAScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.op.structured.OPDFSScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.recovery.RScheduler;
import intellistream.morphstream.engine.db.storage.impl.LocalStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Every thread has its own TxnManager.
 */
public abstract class TxnManager implements ITxnManager {
    private static final Logger log = LoggerFactory.getLogger(TxnManager.class);
    public static LoggingManager loggingManager;
    public static boolean enableGroup = false;
    protected static boolean enableDynamic = false;
    protected static IScheduler scheduler; // TODO: this is a bad encapsulation, try to make it non static, remove after stage is done
    protected static IScheduler recoveryScheduler;
    /**
     * For dynamic workload
     */
    protected static HashMap<String, IScheduler> schedulerPool; // TODO: this is a bad encapsulation, remove after stage is done
    protected static Collector collector = new Collector();
    protected static ConcurrentHashMap<Integer, String> currentSchedulerType = new ConcurrentHashMap<>();
    /**
     * Scheduler for multiple workload
     */
    protected static HashMap<Integer, IScheduler> schedulerByGroup;
    protected static HashMap<Integer, String> schedulerTypeByGroup;
    public static int groupNum;

    public static void initScheduleForStaticWorkload(String schedulerType, int threadCount, int numberOfStates) {
        scheduler = CreateSchedulerByType(schedulerType, threadCount, numberOfStates);
        scheduler.initTPG(0);
        if (loggingManager != null) {
            scheduler.setLoggingManager(loggingManager);
        }
    }
    public static void initSchedulersByGroupForMultipleWorkload(String schedulerType, int threadCount, int numberOfStates) {
        schedulerByGroup = new HashMap<>();
        schedulerTypeByGroup = new HashMap<>();
        String[] scheduler = schedulerType.split(",");
        for (int i = 0; i < scheduler.length; i++) {
            TxnManager.schedulerByGroup.put(i, CreateSchedulerByType(scheduler[i], threadCount / scheduler.length, numberOfStates / scheduler.length));
            TxnManager.schedulerTypeByGroup.put(i, scheduler[i]);
            TxnManager.schedulerByGroup.get(i).initTPG(i * (threadCount / scheduler.length));
            if (loggingManager != null) {
                TxnManager.schedulerByGroup.get(i).setLoggingManager(loggingManager);
            }
        }
        enableGroup = true;
        groupNum = scheduler.length;
    }
    /**
     * Configure the scheduler pool
     */
    public static void initSchedulerPoolForDynamicWorkload(String defaultScheduler, String schedulerPool, int threadCount, int numberOfStates) {
        TxnManager.schedulerPool = new HashMap<>();
        String[] scheduler = schedulerPool.split(",");
        for (int i = 0; i < scheduler.length; i++) {
            TxnManager.schedulerPool.put(scheduler[i], CreateSchedulerByType(scheduler[i], threadCount, numberOfStates));
            TxnManager.schedulerPool.get(scheduler[i]).initTPG(0);
            if (loggingManager != null) {
                TxnManager.schedulerPool.get(scheduler[i]).setLoggingManager(loggingManager);
            }
        }
        for (int i = 0; i < threadCount; i++) {
            TxnManager.currentSchedulerType.put(i, defaultScheduler);
        }
        collector.InitCollector(threadCount);
        TxnManager.scheduler = TxnManager.schedulerPool.get(defaultScheduler);
        log.info("Current Scheduler is " + defaultScheduler + " markId: " + 0);
        enableDynamic = true;
    }
    public static void initRecoveryScheduler(int FTOption, int threadCount, int numberOfStates) {
        if (FTOption == 3) {
            recoveryScheduler = new RScheduler(threadCount, numberOfStates);
            recoveryScheduler.initTPG(0);
            if (loggingManager != null) {
                recoveryScheduler.setLoggingManager(loggingManager);
            }
            scheduler = recoveryScheduler;
        }
    }
    public static void initDScheduler(int totalThreads, int numItems, RdmaWorkerManager rdmaWorkerManager, RemoteStorageManager remoteStorageManager) {
        scheduler = new DSSchedule(totalThreads, numItems, rdmaWorkerManager, remoteStorageManager);
        scheduler.initTPG(0);
        if (loggingManager != null) {
            scheduler.setLoggingManager(loggingManager);
        }
    }

    /**
     * create Scheduler by flag
     *
     * @param schedulerType
     * @param threadCount
     * @param numberOfStates
     * @return
     */
    public static IScheduler CreateSchedulerByType(String schedulerType, int threadCount, int numberOfStates) {
        switch (schedulerType) {
            case "OG_BFS": // Group of operation + Structured BFS exploration strategy + coarse-grained
                return new OGBFSScheduler(threadCount, numberOfStates);
            case "OG_BFS_A": // Group of operation + Structured BFS exploration strategy + fine-grained
                return new OGBFSAScheduler(threadCount, numberOfStates);
            case "OG_DFS": // Group of operation + Structured DFS exploration strategy + coarse-grained
                return new OGDFSScheduler(threadCount, numberOfStates);
            case "OG_DFS_A": // Group of operation + Structured DFS exploration strategy + fine-grained
                return new OGDFSAScheduler(threadCount, numberOfStates);
            case "OG_NS": // Group of operation + Non-structured exploration strategy + coarse-grained
                return new OGNSScheduler(threadCount, numberOfStates);
            case "OG_NS_A": // Group of operation + Non-structured exploration strategy + fine-grained
                return new OGNSAScheduler(threadCount, numberOfStates);
            case "OP_NS": // Single operation + Non-structured exploration strategy + coarse-grained
                return new OPNSScheduler<>(threadCount, numberOfStates);
            case "OP_NS_A": // Single operation + Non-structured exploration strategy + fine-grained
                return new OPNSAScheduler<>(threadCount, numberOfStates);
            case "OP_BFS": // Single operation + Structured BFS exploration strategy + coarse-grained
                return new OPBFSScheduler<>(threadCount, numberOfStates);
            case "OP_BFS_A": // Single operation + Structured BFS exploration strategy + fine-grained
                return new OPBFSAScheduler<>(threadCount, numberOfStates);
            case "OP_DFS": // Single operation + Structured DFS exploration strategy + coarse-grained
                return new OPDFSScheduler<>(threadCount, numberOfStates);
            case "OP_DFS_A": // Single operation + Structured DFS exploration strategy + fine-grained
                return new OPDFSAScheduler<>(threadCount, numberOfStates);
            case "TStream": // original TStream also uses Non-structured exploration strategy
                return new TStreamScheduler(threadCount, numberOfStates);
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + schedulerType);
        }
    }

    /**
     * Configure the bottom line for triggering scheduler switching in Collector
     */
    public static void setBottomLine(String bottomLine) {
        collector.setBottomLine(bottomLine);
    }

    /**
     * Configure the bottom line for triggering scheduler switching in Collector
     */
    public static void setWorkloadConfig(String config) {
        collector.setWorkloadConfig(config);
    }
}
