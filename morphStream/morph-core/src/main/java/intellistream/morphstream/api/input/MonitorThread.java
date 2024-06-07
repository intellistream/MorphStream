package intellistream.morphstream.api.input;

import intellistream.morphstream.api.input.simVNF.VNFRunner;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class MonitorThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorThread.class);
    private static int txnCounter = 0;
    private static int nextPunctuationID = 1; // The next punctuation ID that instances can begin, start from 1
    private static BlockingQueue<PatternData> patternDataQueue;
    private static final ConcurrentHashMap<Integer, Integer> readCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> writeCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> ownershipMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> conflictCountMap = new ConcurrentHashMap<>();
    private static final HashMap<Integer, Integer> stateCurrentPatterns = new HashMap<>(); //0: Low_conflict, 1: Read_heavy, 2: Write_heavy, 3: High_conflict
    private static final HashMap<Integer, Integer> statesPattern_0_to_23 = new HashMap<>(); //state tuples whose pattern changed from 1 to 3/4
    private static final HashMap<Integer, Integer> statesPattern_1_to_23 = new HashMap<>();
    private static final HashMap<Integer, Integer> statesPattern_23_to_0 = new HashMap<>();
    private static final HashMap<Integer, Integer> statesPattern_23_to_1 = new HashMap<>();
    private static final HashMap<Integer, Integer> statesPatternChanges = new HashMap<>(); //Tuples under pattern changes -> new pattern
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int conflictThreshold = MorphStreamEnv.get().configuration().getInt("conflictThreshold");
    private static final int typeThreshold = MorphStreamEnv.get().configuration().getInt("typeThreshold");
    private static final HashMap<Integer, Integer> statePartitionMap = MorphStreamEnv.get().stateInstanceMap();
    private static final int patternPunctuation = MorphStreamEnv.get().configuration().getInt("managerPatternPunctuation");
    private static final int communicationChoice = MorphStreamEnv.get().configuration().getInt("communicationChoice");
    private static final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");

//    private static Map<Integer, Socket> instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
//    private static final Map<Integer, InputStream> instanceInputStreams = new HashMap<>();
//    private static final Map<Integer, OutputStream> instanceOutputStreams = new HashMap<>();
//    private static final ConcurrentHashMap<Integer, Object> instanceLocks = MorphStreamEnv.instanceLocks;


    public MonitorThread(BlockingQueue<PatternData> patternDataQueue) {
        MonitorThread.patternDataQueue = patternDataQueue;
//        instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
//        for (Map.Entry<Integer, Socket> entry : instanceSocketMap.entrySet()) {
//            try {
//                instanceInputStreams.put(entry.getKey(), entry.getValue().getInputStream());
//                instanceOutputStreams.put(entry.getKey(), entry.getValue().getOutputStream());
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    public static void submitPatternData(PatternData patternData) {
        try {
            patternDataQueue.put(patternData);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            PatternData patternData;
            try {
                patternData = patternDataQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (patternData.getInstanceID() == -1) {
                System.out.println("Pattern monitor thread received stop signal");
                break;
            }
            updatePatternData(patternData);
            txnCounter++;
            if (txnCounter % patternPunctuation == 0) {
                nextPunctuationID++;
                LOG.info("Pattern monitor judge pattern changes...");
                judgePattern(); //Determine pattern change for each state tuple that is R/W in the window

                if (!statesPattern_0_to_23.isEmpty() || !statesPattern_1_to_23.isEmpty() || !statesPattern_23_to_0.isEmpty() || !statesPattern_23_to_1.isEmpty()) {
                    notifyCCSwitch();
                    notifyStartNextPunctuation();
                    ccSwitch();
                } else {
                    notifyStartNextPunctuation();
                    LOG.info("No pattern change detected");
                }
                readCountMap.clear();
                writeCountMap.clear();
                conflictCountMap.clear();
                statesPattern_0_to_23.clear();
                statesPattern_1_to_23.clear();
                statesPattern_23_to_0.clear();
                statesPattern_23_to_1.clear();
                statesPatternChanges.clear();
            }
        }
    }

    private static void updatePatternData(PatternData metaDataByte) {
        int instanceID = metaDataByte.getInstanceID();
        int tupleID = metaDataByte.getTupleID();
        boolean isWrite = metaDataByte.getIsWrite();

        readCountMap.merge(tupleID, !isWrite ? 1 : 0, Integer::sum); //read
        writeCountMap.merge(tupleID, isWrite ? 1 : 0, Integer::sum); //write
        Integer currentOwnership = ownershipMap.get(tupleID);
        if (currentOwnership != null && !currentOwnership.equals(instanceID)) {
            conflictCountMap.merge(tupleID, 1, Integer::sum);
        } else {
            conflictCountMap.put(tupleID, 0);
        }
        ownershipMap.put(tupleID, instanceID);
    }

    private static void judgePattern() {
        for (Map.Entry<Integer, Integer> entry : readCountMap.entrySet()) {
            int state = entry.getKey();
            int readCount = entry.getValue();
            int writeCount = writeCountMap.get(state);
            int readRatio = (readCount / (readCount + writeCount)) * 100;
            int writeRatio = (writeCount / (readCount + writeCount)) * 100;
            int conflictCount = conflictCountMap.get(state);
            int oldPattern = stateCurrentPatterns.getOrDefault(state, -1);
            int newPattern;

            if (conflictCount < conflictThreshold) { // Low_conflict
                newPattern = 0;
            } else if (readRatio > typeThreshold) { // Read_heavy
                newPattern = 1;
            } else if (writeRatio > typeThreshold) { // Write_heavy
                newPattern = 2;
            } else { // High_conflict
                newPattern = 3;
            }

            if (newPattern != oldPattern) {
                if (oldPattern == 0 && (newPattern == 2 || newPattern == 3)) {
                    statesPattern_0_to_23.put(state, newPattern);
                } else if (oldPattern == 1 && (newPattern == 2 || newPattern == 3)) {
                    statesPattern_1_to_23.put(state, newPattern);
                } else if ((oldPattern == 2 || oldPattern == 3) && newPattern == 0) {
                    statesPattern_23_to_0.put(state, newPattern);
                } else if ((oldPattern == 2 || oldPattern == 3) && newPattern == 1) {
                    statesPattern_23_to_1.put(state, newPattern);
                }
                stateCurrentPatterns.put(state, newPattern);
                statesPatternChanges.put(state, newPattern); // Only stores states whose pattern changed
            }
        }
    }

    private static void syncTupleToGlobal(int instanceID, int tupleID) {
        System.out.println("Monitor sync state to global " + instanceID + " for tuple: " + tupleID);

        if (communicationChoice == 0) {
            try {
                int tupleValue = VNFRunner.getSender(instanceID).readLocalState(tupleID);
                TableRecord condition_record = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID));
                SchemaRecord srcRecord = condition_record.content_.readPreValues(System.nanoTime());
                SchemaRecord tempo_record = new SchemaRecord(srcRecord);
                tempo_record.getValues().get(1).setInt(tupleValue);
                condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);

            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }

        } else if (communicationChoice == 1) {
            throw new RuntimeException("Communication choice 1 not supported");
        } else {
            throw new RuntimeException("Invalid communication choice");
        }

    }

    private static void syncTupleToLocal(int instanceID, int tupleID) {
        System.out.println("Monitor sync state to local " + instanceID + " for tuple: " + tupleID);
        if (communicationChoice == 0) {
            try {
                int tupleValue;
                TableRecord condition_record = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(tupleID)); //TODO: Specify table name
                tupleValue = (int) condition_record.content_.readPreValues(System.nanoTime()).getValues().get(1).getDouble();
                VNFRunner.getSender(instanceID).writeLocalState(tupleID, tupleValue);

            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }

        } else if (communicationChoice == 1) {
            throw new RuntimeException("Communication choice 1 not supported");
        } else {
            throw new RuntimeException("Invalid communication choice");
        }
    }

    private static void notifyCCSwitch() {
        //Notify instances for pattern change
        for (Map.Entry<Integer, Integer> entry : statesPatternChanges.entrySet()) {
            int tupleID = entry.getKey();
            int newPattern = entry.getValue();
            for (int instanceID = 0; instanceID < vnfInstanceNum; instanceID++) {
                VNFRunner.getSender(instanceID).addTupleCCSwitch(tupleID, newPattern);
            }
        }
    }

    private static void notifyStartNextPunctuation() {
        //Notify instances to start the next punctuation
        for (int instanceID = 0; instanceID < vnfInstanceNum; instanceID++) {
            VNFRunner.getSender(instanceID).notifyNextPuncStart(nextPunctuationID);
        }
    }

    private static void ccSwitch() {
        LOG.info("Monitor starts CC switch");

        //From dedicated local cache partition to global store
        for (Map.Entry<Integer, Integer> entry : statesPattern_0_to_23.entrySet()) {
            int tupleID = entry.getKey();
            syncTupleToGlobal(statePartitionMap.get(tupleID), tupleID);
        }

        //From ANY local cache to global store
        for (Map.Entry<Integer, Integer> entry : statesPattern_1_to_23.entrySet()) {
            syncTupleToGlobal(0, entry.getKey());
        }

        //From global store to dedicated local state partition
        for (Map.Entry<Integer, Integer> entry : statesPattern_23_to_0.entrySet()) {
            int tupleID = entry.getKey();
            syncTupleToLocal(statePartitionMap.get(tupleID), tupleID);
        }

        //From global store to ALL local caches
        for (Map.Entry<Integer, Integer> entry : statesPattern_23_to_1.entrySet()) {
            int tupleID = entry.getKey();
            for (int instanceID = 0; instanceID < vnfInstanceNum; instanceID++) {
                syncTupleToLocal(instanceID, tupleID);
            }
        }

        //Notify instances for state sync completion
        for (Map.Entry<Integer, Integer> entry : statesPatternChanges.entrySet()) {
            int tupleID = entry.getKey();
            VNFRunner.getSender(statePartitionMap.get(tupleID)).endTupleCCSwitch(tupleID, entry.getValue()); //Pattern -> CC: 1-to-1 mapping
        }

        //TODO: State movement optimizations

        LOG.info("Monitor ends CC switch");
    }

}
