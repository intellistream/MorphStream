package intellistream.morphstream.transNFV.state_managers;

import intellistream.morphstream.transNFV.common.VNFRequest;
import intellistream.morphstream.transNFV.vnf.UDF;
import intellistream.morphstream.transNFV.vnf.VNFInstance;
import intellistream.morphstream.transNFV.vnf.VNFManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.*;

public class OpenNFStateManager implements Runnable {
    private BlockingQueue<VNFRequest> requestQueue; // Assume all states are sharing by all instances
    private final StorageManager storageManager = MorphStreamEnv.get().database().getStorageManager();
    private final HashMap<Integer, String> saTableNameMap = MorphStreamEnv.get().getSaTableNameMap();
    private final int numInstances = MorphStreamEnv.get().configuration().getInt("numInstances");
    private long initEndTime = -1;
    private long processEndTime = -1;

    private final boolean enableTimeBreakdown = (MorphStreamEnv.get().configuration().getInt("enableTimeBreakdown") == 1);
    private long usefulStartTime = 0;
    private long parsingStartTime = 0;
    private long AGG_USEFUL_TIME = 0;
    private long AGG_PARSING_TIME = 0;


    public OpenNFStateManager(BlockingQueue<VNFRequest> requestQueue) {
        this.requestQueue = requestQueue;
    }

    public void submitOpenNFReq(VNFRequest request) {
        try {
            requestQueue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        initEndTime = System.nanoTime();

        System.out.println("Broadcasting Controller started.");
        while (!Thread.currentThread().isInterrupted()) {
            VNFRequest request;

            try {
                request = requestQueue.take();
                if (request.getCreateTime() == -1) {
                    processEndTime = System.nanoTime();
//                    writeCSVTimestamps();
                    System.out.println("Broadcasting Controller received stop signal");
                    break;
                }
                int instanceID = request.getInstanceID();
                int tupleID = request.getTupleID();
                String type = request.getType();
                int readValue = -1;

                REC_usefulStartTime();
                VNFInstance targetInstance = VNFManager.getInstance(instanceID);
                targetInstance.getLocalSVCCStateManager().nonBlockingTxnExecution(request);
                REC_usefulEndTime();

                if (Objects.equals(type, "Write")) {
                    for (int i = 0; i < numInstances; i++) {
                        // This simulates a broadcasting of state update results to all instances
                        VNFManager.getInstanceStateManager(i).nonSafeLocalStateUpdate(tupleID, readValue);
                    }
                }

                REC_parsingStartTime();
                VNFManager.getInstance(instanceID).submitACK(request);
                REC_parsingEndTime();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void REC_usefulStartTime() {
        if (enableTimeBreakdown) {
            usefulStartTime = System.nanoTime();
        }
    }

    private void REC_usefulEndTime() {
        if (enableTimeBreakdown) {
            AGG_USEFUL_TIME += System.nanoTime() - usefulStartTime;
        }
    }

    private void REC_parsingStartTime() {
        if (enableTimeBreakdown) {
            parsingStartTime = System.nanoTime();
        }
    }

    private void REC_parsingEndTime() {
        if (enableTimeBreakdown) {
            AGG_PARSING_TIME += System.nanoTime() - parsingStartTime;
        }
    }

    public long getAGG_USEFUL_TIME() {
        return AGG_USEFUL_TIME;
    }

    public long getAGG_PARSING_TIME() {
        return AGG_PARSING_TIME;
    }


}



