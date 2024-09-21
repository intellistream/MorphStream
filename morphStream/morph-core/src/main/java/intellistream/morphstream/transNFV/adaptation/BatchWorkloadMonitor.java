package intellistream.morphstream.transNFV.adaptation;

import intellistream.morphstream.transNFV.common.PatternData;

import java.util.concurrent.BlockingQueue;

public class BatchWorkloadMonitor implements Runnable {
//    private static final Logger LOG = LoggerFactory.getLogger(BatchMonitorThread.class);
//    private static int txnCounter = 0;
//    private static int nextPunctuationID = 1; // The next punctuation ID that instances can begin, start from 1
    private static BlockingQueue<PatternData> patternDataQueue;
//    private static final ConcurrentSkipListSet<Integer> currentBatchTuples = new ConcurrentSkipListSet<>();
//    private static final ConcurrentHashMap<Integer, Integer> readCountMap = new ConcurrentHashMap<>();
//    private static final ConcurrentHashMap<Integer, Integer> writeCountMap = new ConcurrentHashMap<>();
//    private static final ConcurrentHashMap<Integer, Integer> ownershipMap = new ConcurrentHashMap<>();
//    private static final ConcurrentHashMap<Integer, Integer> conflictCountMap = new ConcurrentHashMap<>();
//    private static final HashMap<Integer, String> tupleCurrentPatterns = new HashMap<>(); //0: Low_conflict, 1: Read_heavy, 2: Write_heavy, 3: High_conflict
//    private static final HashMap<Integer, String> tuplePattern_0_to_23 = new HashMap<>(); //state tuples whose pattern changed from 1 to 3/4
//    private static final HashMap<Integer, String> tuplePattern_1_to_23 = new HashMap<>();
//    private static final HashMap<Integer, String> tuplePattern_23_to_0 = new HashMap<>();
//    private static final HashMap<Integer, String> tuplePattern_23_to_1 = new HashMap<>();
//    private static final HashMap<Integer, String> statesPatternChanges = new HashMap<>(); //Tuples under pattern changes -> new pattern
//    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
//    private static final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
//    private static final int instancePuncSize = MorphStreamEnv.get().configuration().getInt("instancePatternPunctuation");
//    private static final int patternPunctuation = numInstances * instancePuncSize;
//    private static final int conflictThreshold = MorphStreamEnv.get().configuration().getInt("conflictThreshold");
//    private static final int typeThreshold = MorphStreamEnv.get().configuration().getInt("typeThreshold");
//    private static final HashMap<Integer, Integer> statePartitionMap = MorphStreamEnv.get().stateInstanceMap();
//    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");


    public BatchWorkloadMonitor(BlockingQueue<PatternData> patternDataQueue) {
        BatchWorkloadMonitor.patternDataQueue = patternDataQueue;
    }

    @Override
    public void run() {}

//    public static void submitPatternData(PatternData patternData) {
//        try {
//            patternDataQueue.put(patternData);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public void run() {
//        while (!Thread.currentThread().isInterrupted()) {
//            PatternData patternData;
//            try {
//                patternData = patternDataQueue.take();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//            if (patternData.getInstanceID() == -1) {
//                System.out.println("Pattern monitor thread received stop signal");
//                break;
//            }
//            updatePatternData(patternData);
//            txnCounter++;
//
//            if (txnCounter % patternPunctuation == 0) {
//                LOG.info("Pattern judge pattern for window " + nextPunctuationID);
//                nextPunctuationID++;
//
//                judgePatternForWindow();
//
//                if (!statesPatternChanges.isEmpty()) {
//                    notifyCCSwitch();
//                    notifyStartNextPunctuation(); //TODO: This is used in a instance-barrier setup, where instances wait for CC switch signals before continuing
//                    ccSwitch();
//                } else {
//                    notifyStartNextPunctuation();
//                    LOG.info("No pattern change detected");
//                }
//
//                readCountMap.clear();
//                writeCountMap.clear();
//                conflictCountMap.clear();
//                tuplePattern_0_to_23.clear();
//                tuplePattern_1_to_23.clear();
//                tuplePattern_23_to_0.clear();
//                tuplePattern_23_to_1.clear();
//                statesPatternChanges.clear();
//                currentBatchTuples.clear();
//            }
//        }
//    }
//
//    private static void updatePatternData(PatternData metaDataByte) {
//        int instanceID = metaDataByte.getInstanceID();
//        int tupleID = metaDataByte.getTupleID();
//        String type = metaDataByte.getType();
//        if (type == "Read") { // Read
//            readCountMap.merge(tupleID, 1, Integer::sum);
//        } else if (type == "Write") { // Write
//            writeCountMap.merge(tupleID, 1, Integer::sum);
//        } else { // Read and Write
//            readCountMap.merge(tupleID, 1, Integer::sum);
//            writeCountMap.merge(tupleID, 1, Integer::sum);
//        }
//        Integer currentOwnership = ownershipMap.get(tupleID);
//        if (currentOwnership != null && !currentOwnership.equals(instanceID)) {
//            conflictCountMap.merge(tupleID, 1, Integer::sum);
//        } else {
//            conflictCountMap.put(tupleID, 0);
//        }
//        ownershipMap.put(tupleID, instanceID);
//    }
//
//    private static void judgePatternForWindow() {
//        for (Integer tupleID : currentBatchTuples) {
//            // Fetch all needed values once
//            int readCount = readCountMap.getOrDefault(tupleID, -1);
//            int writeCount = writeCountMap.getOrDefault(tupleID, -1);
//            int conflictCount = conflictCountMap.getOrDefault(tupleID, 0);
//            int oldPattern = tupleCurrentPatterns.getOrDefault(tupleID, -1);
//
//            int newPattern;
//            if (readCount == -1) { // No read operation, write-heavy
//                newPattern = 2;
//            } else if (writeCount == -1) { // No write operation, read-heavy
//                newPattern = 1;
//            } else {
//                double totalOps = readCount + writeCount;
//                double readRatio = (readCount / totalOps) * 100;
//                double writeRatio = (writeCount / totalOps) * 100;
//
//                if (conflictCount < conflictThreshold) { // Low_conflict
//                    newPattern = 0;
//                } else if (readRatio > typeThreshold) { // Read_heavy
//                    newPattern = 1;
//                } else if (writeRatio > typeThreshold) { // Write_heavy
//                    newPattern = 2;
//                } else { // High_conflict
//                    newPattern = 3; // TODO: Determine this by read-write dependency count (refer to paper)
//                }
//            }
//
//            if (newPattern != oldPattern) {
//                switch (oldPattern) {
//                    case 0:
//                        if (newPattern == 2 || newPattern == 3) {
//                            tuplePattern_0_to_23.put(tupleID, newPattern);
//                        }
//                        break;
//                    case 1:
//                        if (newPattern == 2 || newPattern == 3) {
//                            tuplePattern_1_to_23.put(tupleID, newPattern);
//                        }
//                        break;
//                    case 2:
//                    case 3:
//                        if (newPattern == 0) {
//                            tuplePattern_23_to_0.put(tupleID, newPattern);
//                        } else if (newPattern == 1) {
//                            tuplePattern_23_to_1.put(tupleID, newPattern);
//                        }
//                        break;
//                    default:
//                        break;
//                }
//                tupleCurrentPatterns.put(tupleID, newPattern);
//                statesPatternChanges.put(tupleID, newPattern);
//            }
//        }
//    }
//
//    private static void syncTupleToGlobal(int instanceID, int tupleID) {
//        System.out.println("Monitor sync state to global " + instanceID + " for tuple: " + tupleID);
//
//        if (communicationChoice == 0) {
//            try {
//                int tupleValue = VNFManager.getSender(instanceID).readLocalState(tupleID);
//                TableRecord condition_record = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
//                SchemaRecord srcRecord = condition_record.content_.readPreValues(System.nanoTime());
//                SchemaRecord tempo_record = new SchemaRecord(srcRecord);
//                tempo_record.getValues().get(1).setInt(tupleValue);
//                condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);
//
//            } catch (DatabaseException e) {
//                throw new RuntimeException(e);
//            }
//
//        } else if (communicationChoice == 1) {
//            throw new RuntimeException("Communication choice 1 not supported");
//        } else {
//            throw new RuntimeException("Invalid communication choice");
//        }
//
//    }
//
//    private static void syncTupleToLocal(int instanceID, int tupleID) {
//        System.out.println("Monitor sync state to local " + instanceID + " for tuple: " + tupleID);
//        if (communicationChoice == 0) {
//            try {
//                int tupleValue;
//                TableRecord condition_record = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID)); //TODO: Specify table name
//                tupleValue = (int) condition_record.content_.readPreValues(System.nanoTime()).getValues().get(1).getDouble();
//                VNFManager.getSender(instanceID).writeLocalState(tupleID, tupleValue);
//
//            } catch (DatabaseException e) {
//                throw new RuntimeException(e);
//            }
//
//        } else if (communicationChoice == 1) {
//            throw new RuntimeException("Communication choice 1 not supported");
//        } else {
//            throw new RuntimeException("Invalid communication choice");
//        }
//    }
//
//    private static void notifyCCSwitch() {
//        //Notify instances for pattern change
//        for (Map.Entry<Integer, Integer> entry : statesPatternChanges.entrySet()) {
//            int tupleID = entry.getKey();
//            int newPattern = entry.getValue();
//            for (int instanceID = 0; instanceID < numInstances; instanceID++) {
//                VNFManager.getSender(instanceID).addTupleCCSwitch(tupleID, newPattern);
//            }
//        }
//    }
//
//    private static void notifyStartNextPunctuation() {
//        //Notify instances to start the next punctuation
//        for (int instanceID = 0; instanceID < numInstances; instanceID++) {
//            VNFManager.getSender(instanceID).notifyNextPuncStart(nextPunctuationID);
//        }
//    }
//
//    private static void ccSwitch() {
//        LOG.info("Monitor starts CC switch"); //TODO: Measure CC switch time here or at instance?
//
//        //From dedicated local cache partition to global store
//        for (Map.Entry<Integer, String> entry : tuplePattern_0_to_23.entrySet()) {
//            int tupleID = entry.getKey();
//            syncTupleToGlobal(statePartitionMap.get(tupleID), tupleID);
//        }
//
//        //From ANY local cache to global store
//        for (Map.Entry<Integer, String> entry : tuplePattern_1_to_23.entrySet()) {
//            syncTupleToGlobal(0, entry.getKey());
//        }
//
//        //From global store to dedicated local state partition
//        for (Map.Entry<Integer, String> entry : tuplePattern_23_to_0.entrySet()) {
//            int tupleID = entry.getKey();
//            syncTupleToLocal(statePartitionMap.get(tupleID), tupleID);
//        }
//
//        //From global store to ALL local caches
//        for (Map.Entry<Integer, String> entry : tuplePattern_23_to_1.entrySet()) {
//            int tupleID = entry.getKey();
//            for (int instanceID = 0; instanceID < numInstances; instanceID++) {
//                syncTupleToLocal(instanceID, tupleID);
//            }
//        }
//
//        //Notify instances for state sync completion
//        for (Map.Entry<Integer, String> entry : statesPatternChanges.entrySet()) {
//            int tupleID = entry.getKey();
//            VNFManager.getSender(statePartitionMap.get(tupleID)).endTupleCCSwitch(tupleID, entry.getValue()); //Pattern -> CC: 1-to-1 mapping
//        }
//
//        //TODO: State movement optimizations
//
//        LOG.info("Monitor ends CC switch");
//    }

}
