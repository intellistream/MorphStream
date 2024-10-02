package intellistream.morphstream.transNFV.adaptation;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.transNFV.common.PatternData;
import intellistream.morphstream.transNFV.vnf.VNFInstance;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IterativeWorkloadMonitor implements Runnable {
    private final Logger LOG = LoggerFactory.getLogger(IterativeWorkloadMonitor.class);
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private final int numItems = MorphStreamEnv.get().configuration().getInt("NUM_ITEMS");
    private final int monitorWindowSize = MorphStreamEnv.get().configuration().getInt("monitorWindowSize");
//    private final int outstandingKeyInterval = MorphStreamEnv.get().configuration().getInt("outstandingKeyInterval");
    private final int partitionSize = numItems / numInstances;

    private final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, Integer> statePartitionMap = MorphStreamEnv.get().stateInstanceMap();

    private BlockingQueue<PatternData> patternDataQueue;
    private AtomicInteger txnCounter = new AtomicInteger(0);
    private int nextPunctuationID = 1; // The next punctuation ID that instances can begin, start from 1

    private final ConcurrentHashMap<Integer, Integer> keyReadCounterMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Integer> keyWriteCounterMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Integer> keyCongestionLevelMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ConcurrentHashMap<Integer, Integer>> keyInstanceLocalityMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Integer> instanceReqCounterMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Integer> instanceScopeRatioMap = new ConcurrentHashMap<>();



    public IterativeWorkloadMonitor(BlockingQueue<PatternData> patternDataQueue) {
        this.patternDataQueue = patternDataQueue;
        for (int i = 0; i < numItems; i++) {
            keyReadCounterMap.put(i, 0);
            keyCongestionLevelMap.put(i, 0);
            keyInstanceLocalityMap.put(i, new ConcurrentHashMap<>());
            for (int j = 0; j < numInstances; j++) {
                keyInstanceLocalityMap.get(i).put(j, 0);
            }
        }
        for (int i = 0; i < numInstances; i++) {
            instanceReqCounterMap.put(i, 0);
            instanceScopeRatioMap.put(i, 0);
        }
        try {
            PerformanceModel.loadModel();
        } catch (JAXBException | IOException | SAXException e) {
            throw new RuntimeException(e);
        }
    }

    public void submitMetadata(int key, int instanceID, String accessType, String scope) {
        txnCounter.incrementAndGet(); //TODO: This could slow down the performance
        instanceReqCounterMap.compute(key, (k, v) -> (v == null) ? 1 : v + 1);
        patternDataQueue.add(new PatternData(key, instanceID, accessType));
        //TODO: Complete the logic for workload char update
    }

    @Override
    public void run() {
        while (true) {
            PatternData patternData = null;
            try {
                patternData = patternDataQueue.take();
                if (patternData.getTupleID() == -1) {
                    System.out.println("Monitor thread received stop signal");
                    break;
                }

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            try {
                int key = patternData.getTupleID();
                // TODO: Perform workload char analysis for the key
                double keySkew = 0;
                double workloadSkew = 0;
                double readRatio = 0;
                double locality = 0;
                double scopeRatio = 0;

                String predictedOptimalStrategy = PerformanceModel.predictOptimalStrategy(keySkew, workloadSkew, readRatio, locality, scopeRatio);

                if (txnCounter.get() % monitorWindowSize == 0) {
                    for (VNFInstance instance : VNFManager.getAllInstances().values()) {
                        instance.startTupleCCSwitch(key, "Invalid"); //TODO: Update the correct CC type, so that instance can switch to optimal CC
                        TableRecord tableRecord = storageManager.getTable("testTable").SelectKeyRecord(String.valueOf(key)); //TODO: Simulated state synchronization
                        int value = tableRecord.content_.readPreValues(Long.MAX_VALUE).getValues().get(1).getInt();
                        instance.endTupleCCSwitch(key, "Invalid");
                    }
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


}
