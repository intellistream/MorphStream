package intellistream.morphstream.transNFV.adaptation;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.transNFV.common.PatternData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class IterativeWorkloadMonitor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(IterativeWorkloadMonitor.class);
    private static int txnCounter = 0;
    private static int nextPunctuationID = 1; // The next punctuation ID that instances can begin, start from 1
    private static BlockingQueue<PatternData> patternDataQueue;
    private static final ConcurrentSkipListSet<Integer> currentBatchTuples = new ConcurrentSkipListSet<>();
    private static final ConcurrentHashMap<Integer, Integer> readCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> writeCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> ownershipMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> conflictCountMap = new ConcurrentHashMap<>();
    private static final HashMap<Integer, String> tupleCurrentPatterns = new HashMap<>(); //0: Low_conflict, 1: Read_heavy, 2: Write_heavy, 3: High_conflict
    private static final HashMap<Integer, String> tuplePattern_0_to_23 = new HashMap<>(); //state tuples whose pattern changed from 1 to 3/4
    private static final HashMap<Integer, String> tuplePattern_1_to_23 = new HashMap<>();
    private static final HashMap<Integer, String> tuplePattern_23_to_0 = new HashMap<>();
    private static final HashMap<Integer, String> tuplePattern_23_to_1 = new HashMap<>();
    private static final HashMap<Integer, String> statesPatternChanges = new HashMap<>(); //Tuples under pattern changes -> new pattern
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private static final int conflictThreshold = MorphStreamEnv.get().configuration().getInt("conflictThreshold");
    private static final int typeThreshold = MorphStreamEnv.get().configuration().getInt("typeThreshold");
    private static final HashMap<Integer, Integer> statePartitionMap = MorphStreamEnv.get().stateInstanceMap();
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");


    public IterativeWorkloadMonitor(BlockingQueue<PatternData> patternDataQueue) {
        IterativeWorkloadMonitor.patternDataQueue = patternDataQueue;
    }

    @Override
    public void run() {}


}
