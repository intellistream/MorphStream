package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.util.datatypes.TxnMetaData;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import intellistream.morphstream.util.libVNFFrontend.NativeInterface;


public class PatternMonitor implements Runnable {
    /**
     * Below 4 hashmaps store per-window pattern data for each state-tuple
     * */
    private final ConcurrentHashMap<String, Integer> readCountMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> writeCountMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> ownershipMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> conflictCountMap = new ConcurrentHashMap<>();
    private final HashMap<String, Integer> statePatterns = new HashMap<>(); //0: Low_conflict, 1: Read_heavy, 2: Write_heavy, 3: High_conflict
    private final HashMap<String, Integer> changedStatesOldPatterns = new HashMap<>();
    private final HashMap<String, Integer> changedStatesNewPatterns = new HashMap<>();
    private final HashMap<String, Integer> statesFromLocalToRemote = new HashMap<>(); //state tuples whose pattern changed from 0/1 (local state cache) to 2/3 (global state)
    private final HashMap<String, Integer> statesFromRemoteToLocal = new HashMap<>(); //state tuples whose pattern changed from 2/3 (global state) to 0/1 (local state cache)
    private final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();

    private final int conflictThreshold = 10;
    private final int typeThreshold = 10;

    private ConcurrentLinkedDeque<TxnMetaData> txnMetaDataQueue;
    private int punctuation_interval;
    private int txnCounter;

    public PatternMonitor() {
        txnMetaDataQueue = new ConcurrentLinkedDeque<>();
        txnCounter = 0;
        punctuation_interval = 100;
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            TxnMetaData metaData = txnMetaDataQueue.pollFirst();
            updatePatternData(metaData);
            txnCounter++;
            if (txnCounter % punctuation_interval == 0) {
                judgePattern(); //Determine pattern change for each state tuple that is R/W in the window
//                if (!changedStatesNewPatterns.isEmpty()) {
//                    ccSwitch();
//                }
                if (!statesFromLocalToRemote.isEmpty() || !statesFromRemoteToLocal.isEmpty()) {
                    ccSwitch();
                }
                readCountMap.clear();
                writeCountMap.clear();
                conflictCountMap.clear();
//                changedStatesOldPatterns.clear();
//                changedStatesNewPatterns.clear();
                statesFromLocalToRemote.clear();
                statesFromRemoteToLocal.clear();
            }
        }
    }


    // Called by VNF instances
    public void addTxnMetaData(TxnMetaData txnMetaData) { //TODO: Align format with VNF instances
        txnMetaDataQueue.add(txnMetaData);
    }


    private void updatePatternData(TxnMetaData txnData) {
        HashMap<String, Integer> operations = txnData.getOperationMap(); //TODO: The txnData datastructure could be optimized
        for (Map.Entry<String, Integer> operation : operations.entrySet()) {
            String tupleID = operation.getKey();
            Integer type = operation.getValue();
            readCountMap.merge(tupleID, type==0 ? 1 : 0, Integer::sum); //read
            writeCountMap.merge(tupleID, type==1 ? 1 : 0, Integer::sum); //write

            Integer currentOwnership = ownershipMap.get(tupleID);
            Integer ownership = txnData.getInstanceID();
            if (currentOwnership != null && !currentOwnership.equals(ownership)) {
                conflictCountMap.merge(tupleID, 1, Integer::sum);
            }
            ownershipMap.put(tupleID, ownership);
        }
    }

    private void judgePattern() {

        for (Map.Entry<String, Integer> entry : readCountMap.entrySet()) {
            String state = entry.getKey();
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

//            if (currentPattern != newPattern) {
//                statePatterns.put(state, newPattern);
//                changedStatesOldPatterns.put(state, currentPattern);
//                changedStatesNewPatterns.put(state, newPattern);
//            }
        }
    }

    private void ccSwitch() {
        NativeInterface.__pause_txn_processing(statesFromLocalToRemote, statesFromRemoteToLocal);

        //TODO: State movement optimizations
        //State movement from VNF instance local cache to DB global store
        for (Map.Entry<String, Integer> entry : statesFromLocalToRemote.entrySet()) {
            String tupleID = entry.getKey();
            int value = NativeInterface.__get_state_from_cache(tupleID);
            try {
                TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(tupleID);
                SchemaRecord srcRecord = condition_record.content_.readPreValues(-1); //TODO: Pass-in a valid bid.
                SchemaRecord tempo_record = new SchemaRecord(srcRecord);
                tempo_record.getValues().get(1).setInt(value);
                condition_record.content_.updateMultiValues(-1, 0, true, tempo_record);

            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        }
        //State movement from DB global store to VNF instance local cache
        for (Map.Entry<String, Integer> entry : statesFromRemoteToLocal.entrySet()) {
            String tupleID = entry.getKey();
            try {
                TableRecord condition_record = storageManager.getTable("table").SelectKeyRecord(tupleID); //TODO: Specify table name
                int value = (int) condition_record.content_.readPreValues(Long.MAX_VALUE).getValues().get(1).getDouble(); //read the latest state version
                NativeInterface.__update_states_to_cache(tupleID, value);

            } catch (DatabaseException e) {
                throw new RuntimeException(e);
            }
        }

        NativeInterface.__resume_txn_processing(); //TODO: This could be removed by implementing cc switch as a non-blocking process
    }
}
