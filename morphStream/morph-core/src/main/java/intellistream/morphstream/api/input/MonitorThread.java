package intellistream.morphstream.api.input;

import message.VNFCtlStub;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class MonitorThread implements Runnable {
    private static BlockingQueue<PatternData> patternDataQueue;
    private static final ConcurrentHashMap<Integer, Integer> readCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> writeCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> ownershipMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> conflictCountMap = new ConcurrentHashMap<>();
    private static final HashMap<Integer, Integer> stateCurrentPatterns = new HashMap<>(); //0: Low_conflict, 1: Read_heavy, 2: Write_heavy, 3: High_conflict
    private static final HashMap<Integer, Integer> statesPattern_1_to_34 = new HashMap<>(); //state tuples whose pattern changed from 1 to 3/4
    private static final HashMap<Integer, Integer> statesPattern_2_to_34 = new HashMap<>();
    private static final HashMap<Integer, Integer> statesPattern_34_to_1 = new HashMap<>();
    private static final HashMap<Integer, Integer> statesPattern_34_to_2 = new HashMap<>();
    private static final HashMap<Integer, Integer> statesPatternChanges = new HashMap<>(); //Only stores states whose pattern changed
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int conflictThreshold = 10;
    private static final int typeThreshold = 10;
    private static int txnCounter = 0;
    private static int punctuation_interval;
    private static Map<Integer, Socket> instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    private static final Map<Integer, InputStream> instanceInputStreams = new HashMap<>();
    private static final Map<Integer, OutputStream> instanceOutputStreams = new HashMap<>();
    private static final HashMap<Integer, Integer> statePartitionMap = MorphStreamEnv.get().stateInstanceMap();
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);


    public MonitorThread(BlockingQueue<PatternData> patternDataQueue, int punctuation_interval) {
        this.patternDataQueue = patternDataQueue;
        instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
        MonitorThread.punctuation_interval = punctuation_interval;

        for (Map.Entry<Integer, Socket> entry : instanceSocketMap.entrySet()) {
            try {
                instanceInputStreams.put(entry.getKey(), entry.getValue().getInputStream());
                instanceOutputStreams.put(entry.getKey(), entry.getValue().getOutputStream());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
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
            if (patternData.getTimeStamp() == -1) {
                System.out.println("Pattern monitor thread received stop signal");
                break;
            }
            updatePatternData(patternData);
            txnCounter++;
            if (txnCounter % punctuation_interval == 0) {
                judgePattern(); //Determine pattern change for each state tuple that is R/W in the window

                if (!statesPattern_1_to_34 .isEmpty() || !statesPattern_2_to_34.isEmpty() || !statesPattern_34_to_1.isEmpty() || !statesPattern_34_to_2.isEmpty()) {
                    ccSwitch();
                }
                readCountMap.clear();
                writeCountMap.clear();
                conflictCountMap.clear();
                statesPattern_1_to_34.clear();
                statesPattern_2_to_34.clear();
                statesPattern_34_to_1.clear();
                statesPattern_34_to_2.clear();
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
        }
        ownershipMap.put(tupleID, instanceID);
    }

    private static void judgePattern() {
        for (Map.Entry<Integer, Integer> entry : readCountMap.entrySet()) {
            int state = entry.getKey();
            int readCount = entry.getValue();
            int writeCount = writeCountMap.get(state);
            int conflictCount = conflictCountMap.get(state);
            int oldPattern = stateCurrentPatterns.getOrDefault(state, -1);
            int newPattern;

            if (conflictCount < conflictThreshold) { // Low_conflict
                newPattern = 1;
            } else if (readCount - writeCount > typeThreshold) { // Read_heavy
                newPattern = 2;
            } else if (writeCount - readCount > typeThreshold) { // Write_heavy
                newPattern = 3;
            } else { // High_conflict
                newPattern = 4;
            }

            if (newPattern != oldPattern) {
                if (oldPattern == 1 && (newPattern == 3 || newPattern == 4)) {
                    statesPattern_1_to_34.put(state, newPattern);
                } else if (oldPattern == 2 && (newPattern == 3 || newPattern == 4)) {
                    statesPattern_2_to_34.put(state, newPattern);
                } else if ((oldPattern == 3 || oldPattern == 4) && newPattern == 1) {
                    statesPattern_34_to_1.put(state, newPattern);
                } else if ((oldPattern == 3 || oldPattern == 4) && newPattern == 2) {
                    statesPattern_34_to_2.put(state, newPattern);
                }
                stateCurrentPatterns.put(state, newPattern);
                statesPatternChanges.put(state, newPattern); // Only stores states whose pattern changed
            }
        }
    }

    private static void notifyCCSwitch(int instanceID, int tupleID, int newPattern) {
//        try {
//
//            //TODO: Does this have to be split into two calls?
//            VNFCtrlClient.make_pause();// TODO: Align with libVNF
//            VNFCtrlClient.update_cc(tupleID, newPattern);// TODO: Align with libVNF
//
//        } catch (IOException e) {
//            System.err.println("Error communicating with server on instance " + instanceID + ": " + e.getMessage());
//        }
    }

    private static void notifyStateSyncComplete(int instanceID, int tupleID) {
//        try {
//            // TODO: Align with libVNF
//            VNFCtrlClient.make_continue();
//
//        } catch (IOException e) {
//            System.err.println("Error communicating with server on instance " + instanceID + ": " + e.getMessage());
//        }
    }

    private static void syncStateFromLocalToRemote(int instanceID, int tupleID) {
//        try {
//            System.out.println("Monitor sending state sync to instance " + instanceID + " for tuple: " + tupleID);
//
//            int cachedValue = VNFCtrlClient.fetch_value(tupleID); // TODO: Align with libVNF
//
//            try {
//                TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(String.valueOf(tupleID));
//                SchemaRecord srcRecord = condition_record.content_.readPreValues(-1); //TODO: Pass-in a valid bid.
//                SchemaRecord tempo_record = new SchemaRecord(srcRecord);
//                tempo_record.getValues().get(1).setInt(cachedValue);
//                condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);
//
//            } catch (DatabaseException e) {
//                throw new RuntimeException(e);
//            }
//
//        } catch (IOException e) {
//            System.err.println("Error communicating with server on instance " + instanceID + ": " + e.getMessage());
//        }
    }

    private static void syncStateFromGlobalToLocal(int instanceID, int tupleID) {
//        try {
//            int tupleValue;
//            try {
//                TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(String.valueOf(tupleID)); //TODO: Specify table name
//                tupleValue = (int) condition_record.content_.readPreValues(Long.MAX_VALUE).getValues().get(1).getDouble(); //TODO: Read the latest state version?
//
//            } catch (DatabaseException e) {
//                throw new RuntimeException(e);
//            }
//
//            VNFCtrlClient.update_value(tupleID, tupleValue); // TODO: Align with libVNF
//
//        } catch (IOException e) {
//            System.err.println("Error communicating with server on instance " + instanceID + ": " + e.getMessage());
//        }
    }

    private static void ccSwitch() {

        //Notify instances for pattern change
        for (Map.Entry<Integer, Integer> entry : statesPatternChanges.entrySet()) {
            int tupleID = entry.getKey();
            int newPattern = entry.getValue();
            int instanceID = statePartitionMap.get(tupleID);
            notifyCCSwitch(instanceID, tupleID, newPattern);
        }

        //From dedicated local cache partition to global store
        for (Map.Entry<Integer, Integer> entry : statesPattern_1_to_34.entrySet()) {
            int tupleID = entry.getKey();
            int instanceID = statePartitionMap.get(tupleID);
            syncStateFromLocalToRemote(instanceID, tupleID);
        }

        //From ANY local cache to global store
        for (Map.Entry<Integer, Integer> entry : statesPattern_2_to_34.entrySet()) {
            int tupleID = entry.getKey();
            int instanceID = 0; //TODO: Only requires one instance's cache, make sure it is correct
            syncStateFromLocalToRemote(instanceID, tupleID);
        }

        //From global store to dedicated local state partition
        for (Map.Entry<Integer, Integer> entry : statesPattern_34_to_1.entrySet()) {
            int tupleID = entry.getKey();
            int instanceID = statePartitionMap.get(tupleID);
            syncStateFromGlobalToLocal(instanceID, tupleID);
        }

        //From global store to ALL local caches
        for (Map.Entry<Integer, Integer> entry : statesPattern_34_to_2.entrySet()) {
            int tupleID = entry.getKey();
            for (int instanceID : instanceSocketMap.keySet()) {
                syncStateFromGlobalToLocal(instanceID, tupleID);
            }
        }

        //Notify instances for state sync completion
        for (Map.Entry<Integer, Integer> entry : statesPatternChanges.entrySet()) {
            int tupleID = entry.getKey();
            int instanceID = statePartitionMap.get(tupleID);
            notifyStateSyncComplete(instanceID, tupleID);
        }

        //TODO: State movement optimizations

    }

}
