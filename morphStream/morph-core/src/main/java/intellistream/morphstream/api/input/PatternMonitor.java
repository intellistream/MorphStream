package intellistream.morphstream.api.input;

import intellistream.morphstream.util.datatypes.TxnMetaData;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import intellistream.morphstream.util.libVNFFrontend.NativeInterface;


public class PatternMonitor implements Runnable {
    private ConcurrentHashMap<String, Integer> readCountMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> writeCountMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> ownershipMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Integer> conflictCountMap = new ConcurrentHashMap<>();
    private HashMap<String, Integer> patternMap = new HashMap<>(); //0: Low_conflict, 1: Read_heavy, 2: Write_heavy, 3: High_conflict
    private HashMap<String, Integer> changedOldPatterns = new HashMap<>();
    private HashMap<String, Integer> changedNewPatterns = new HashMap<>();

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
                judgePattern();
                if (!changedNewPatterns.isEmpty()) {
                    ccSwitch();
                }
                readCountMap.clear();
                writeCountMap.clear();
                conflictCountMap.clear();
                changedOldPatterns.clear();
                changedNewPatterns.clear();
            }
        }
    }


    // Called by VNF instances
    public void addTxnMetaData(TxnMetaData txnMetaData) { //TODO: Align format with VNF instances
        txnMetaDataQueue.add(txnMetaData);
    }


    private void updatePatternData(TxnMetaData txnData) {
        HashMap<String, Integer> operations = txnData.getOperationMap();
        for (Map.Entry<String, Integer> operation : operations.entrySet()) {
            String tupleID = operation.getKey();
            Integer type = operation.getValue();
            readCountMap.merge(tupleID, type==0 ? 1 : 0, Integer::sum);
            writeCountMap.merge(tupleID, type==1 ? 1 : 0, Integer::sum);

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
            Integer currentPattern = patternMap.getOrDefault(state, -1);
            Integer newPattern = currentPattern;

            if (conflictCount < conflictThreshold) { // Low_conflict
                newPattern = 0;
            } else if (readCount - writeCount > typeThreshold) { // Read_heavy
                newPattern = 1;
            } else if (writeCount - readCount > typeThreshold) { // Write_heavy
                newPattern = 2;
            } else { // High_conflict
                newPattern = 3;
            }

            if (currentPattern != newPattern) {
                patternMap.put(state, newPattern);
                changedOldPatterns.put(state, currentPattern);
                changedNewPatterns.put(state, newPattern);
            }
        }
    }

    private void ccSwitch() {
        NativeInterface.__pause_txn_processing(changedNewPatterns);
        //TODO: State movement optimizations
        NativeInterface.__resume_txn_processing();
    }
}
