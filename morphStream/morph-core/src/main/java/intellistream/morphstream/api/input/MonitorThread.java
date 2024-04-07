package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.util.datatypes.TxnMetaData;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class MonitorThread implements Runnable {
    private final BlockingQueue<byte[]> txnMetaDataQueue;
    private static Map<Integer, Socket> instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
    private static final ConcurrentHashMap<Integer, Integer> readCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> writeCountMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> ownershipMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Integer, Integer> conflictCountMap = new ConcurrentHashMap<>();
    private static final HashMap<Integer, Integer> statePatterns = new HashMap<>(); //0: Low_conflict, 1: Read_heavy, 2: Write_heavy, 3: High_conflict
    private static final HashMap<Integer, Integer> statesFromLocalToRemote = new HashMap<>(); //state tuples whose pattern changed from 0/1 (local state cache) to 2/3 (global state)
    private static final HashMap<Integer, Integer> statesFromRemoteToLocal = new HashMap<>(); //state tuples whose pattern changed from 2/3 (global state) to 0/1 (local state cache)
    private static final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private static final int conflictThreshold = 10;
    private static final int typeThreshold = 10;
    private static int txnCounter = 0;
    private static int punctuation_interval;

    public MonitorThread(BlockingQueue<byte[]> txnMetaDataQueue, int punctuation_interval) {
        this.txnMetaDataQueue = txnMetaDataQueue;
        instanceSocketMap = MorphStreamEnv.get().instanceSocketMap();
        MonitorThread.punctuation_interval = punctuation_interval;
    }


    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            byte[] metaDataByte = txnMetaDataQueue.poll();
            updatePatternData(metaDataByte);
            txnCounter++;
            if (txnCounter % punctuation_interval == 0) {
                judgePattern(); //Determine pattern change for each state tuple that is R/W in the window

                if (!statesFromLocalToRemote.isEmpty() || !statesFromRemoteToLocal.isEmpty()) {
                    ccSwitch();
                }
                readCountMap.clear();
                writeCountMap.clear();
                conflictCountMap.clear();
                statesFromLocalToRemote.clear();
                statesFromRemoteToLocal.clear();
            }
        }
    }


    private static void updatePatternData(byte[] metaDataByte) {

        //TODO: Add for loop to iterate over operations in txnData
        int instanceID = 0; //TODO: Hardcoded
        int tupleID = 0;
        int type = 0;

        readCountMap.merge(tupleID, type==0 ? 1 : 0, Integer::sum); //read
        writeCountMap.merge(tupleID, type==1 ? 1 : 0, Integer::sum); //write
        Integer currentOwnership = ownershipMap.get(tupleID);
        if (currentOwnership != null && !currentOwnership.equals(instanceID)) {
            conflictCountMap.merge(tupleID, 1, Integer::sum);
        }
        ownershipMap.put(tupleID, instanceID);
    }

    private static void judgePattern() {
        for (Map.Entry<Integer, Integer> entry : readCountMap.entrySet()) {
            int state = entry.getKey();
            Integer readCount = entry.getValue();
            Integer writeCount = writeCountMap.get(state);
            Integer conflictCount = conflictCountMap.get(state);
            Integer oldPattern = statePatterns.getOrDefault(state, -1);
            Integer newPattern;

            if (conflictCount < conflictThreshold) { // Low_conflict
                newPattern = 0;
            } else if (readCount - writeCount > typeThreshold) { // Read_heavy
                newPattern = 1;
            } else if (writeCount - readCount > typeThreshold) { // Write_heavy
                newPattern = 2;
            } else { // High_conflict
                newPattern = 3;
            }

            // Prepare for state movement
            if (oldPattern == 0 || oldPattern == 1) {
                if (newPattern == 2 || newPattern == 3) {
                    statesFromLocalToRemote.put(state, newPattern);
                }
            } else if (oldPattern == 2 || oldPattern == 3) {
                if (newPattern == 0 || newPattern == 1) {
                    statesFromRemoteToLocal.put(state, newPattern);
                }
            }

            if (oldPattern != newPattern) {
                statePatterns.put(state, newPattern);
            }
        }
    }

    private static void ccSwitch() {

        //TODO: Notify VNF instances for state pattern change. This should be done during iteration of statePatternChangeMap.

        //TODO: State movement optimizations
        //State movement from VNF instance local cache to DB global store
        for (Map.Entry<Integer, Integer> entry : statesFromLocalToRemote.entrySet()) {
            Integer tupleID = entry.getKey();

            //TODO: Create a separate thread to listen to VNF instances' cache states?
            int value = 0;

            try {
                TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(String.valueOf(tupleID));
                SchemaRecord srcRecord = condition_record.content_.readPreValues(-1); //TODO: Pass-in a valid bid.
                SchemaRecord tempo_record = new SchemaRecord(srcRecord);
                tempo_record.getValues().get(1).setInt(value);
                condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);

            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        }

        //State movement from DB global store to VNF instance local cache
        for (Map.Entry<Integer, Integer> entry : statesFromRemoteToLocal.entrySet()) {
            Integer tupleID = entry.getKey();
            try {
                TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(String.valueOf(tupleID)); //TODO: Specify table name
                int value = (int) condition_record.content_.readPreValues(Long.MAX_VALUE).getValues().get(1).getDouble(); //read the latest state version

                //TODO: Update instance state caches, separate for Partitioning and Caching

            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        }

        //TODO: Release the buffered operations at pattern monitor to CC threads.
    }

}
