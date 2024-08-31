package message;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.api.input.TransactionalVNFEvent;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.transNFV.*;
import intellistream.morphstream.transNFV.data.PatternData;
import org.example.protobuf.*;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class VNFCtlStubImpl {
    // Callbacks on VNF messages.
    private static ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private static int tpgReqCount = 0;
    private static final int numSpouts = MorphStreamEnv.get().configuration().getInt("tthread");
    private static final int vnfInstanceNum = MorphStreamEnv.get().configuration().getInt("vnfInstanceNum");
    private static final HashMap<Integer, Integer> instanceReqCounters = new HashMap<>();
    private static final int[] partitionReqCountPerInstance = new int[vnfInstanceNum];
    private static final int[] cacheReqCountPerInstance = new int[vnfInstanceNum];
    private static final int[] tpgReqCountPerInstance = new int[vnfInstanceNum];
    private static final int[] tpgReqCountPerQueue = new int[numSpouts];

    public static void initializeVNFCtrlStubImpl() {
        for (int i=0; i<vnfInstanceNum; i++) {
            instanceReqCounters.put(i, 0);
        }
    }

    /** Offloading CC, TPG CC, submit txn req to executor */
    static public void onTxnReqMessage(int instanceID, TxnReqMessage msg) {
        if (msg.getCc().getNumber() == 0) { // Partition
//            PartitionCCThread.submitPartitionRequest(
//                    new PartitionData(System.nanoTime(), msg.getId(), instanceID, msg.getKey(), -1, msg.getSaIdx(), -1));
            partitionReqCountPerInstance[instanceID]++;
//            System.out.println("Server received Partition_Req from client: " + msg.getId());
            if (partitionReqCountPerInstance[instanceID] > 7400) {
                System.out.println("Instance"+instanceID+" has sent " + partitionReqCountPerInstance[instanceID] + " partition req");
            }

        } else if (msg.getCc().getNumber() == 2) { // Offloading
//            OffloadCCThread.submitOffloadReq(
//                    new OffloadData(System.nanoTime(), instanceID, msg.getId(), msg.getKey(), 0, msg.getSaIdx(), 0, -1, -1));
            System.out.println("Server received Offloading_Req from client: " + msg.getId());

        } else if (msg.getCc().getNumber() == 3) { // TPG
            tpgQueues.get(tpgReqCountPerInstance[instanceID] % numSpouts).offer(
                    new TransactionalVNFEvent(-1, instanceID, -1, System.nanoTime(), msg.getId(), msg.getKey(), 0, msg.getSaIdx(), -1));
            tpgReqCountPerInstance[instanceID]++;
            tpgReqCountPerQueue[tpgReqCountPerInstance[instanceID] % numSpouts]++;

            if (tpgReqCountPerInstance[instanceID] == 10000) {
                System.out.println("Instance"+instanceID+" has sent 10000 tpg req");
                for (int k=0; k<numSpouts; k++) {
                    System.out.println("Queue"+k+" has received req: " + tpgReqCountPerQueue[k]);
                }
            }

        } else if (msg.getCc().getNumber() == 4) { // OpenNF broadcasting
//            OpenNFController.submitOpenNFReq(
//                    new OffloadData(System.nanoTime(), instanceID, msg.getId(), msg.getKey(), 0, msg.getSaIdx(), 0, -1, -1));
            System.out.println("Server received OpenNF_Req from client: " + msg.getId());

        } else if (msg.getCc().getNumber() == 5) { // CHC
//            CHCController.submitCHCReq(
//                    new OffloadData(System.nanoTime(), instanceID, msg.getId(), msg.getKey(), 0, msg.getSaIdx(), 0, -1, -1));
            System.out.println("Server received CHC_Req from client: " + msg.getId());
        }

    }

    /** VNF initialization */
    static public void onSFCJsonMessage(int instanceID, SFCMessage msg) {
        MorphStreamEnv.get().vnfJSON = msg.getSFCJson();
        System.out.println("Server received SFCJson from client: " + msg.getSFCJson());
    }

    /** Monitor pattern report */
    static public void onMonitorReportMessage(int instanceID, MonitorReportMessage msg) {
        BatchMonitorThread.submitPatternData(new PatternData(instanceID, msg.getKey(), -1));
//        System.out.println("Server received MonitorReport from client: " + msg.getCcValue());
    }

    /** Currently no use */
    static public void onPushCCMessage(int instanceID, setCCMessage msg) {
        System.out.println("Server received PushCC from client: " + msg.getCcValue());
    }

    /** Cache CC, submit state sync to Cache CC executor */
    static public void onPushDSMessage(int instanceID, setDSMessage msg) {
        System.out.println("Received instance push_DS for tuple " + msg.getKey() + ", value: " + msg.getValue());
        cacheReqCountPerInstance[instanceID]++;
//        CacheCCThread.submitReplicationRequest(new CacheData(0, System.nanoTime(), instanceID, msg.getKey(), msg.getValue(), -1));
//        if (cacheReqCountPerInstance[instanceID] > 7400) {
//            System.out.println("Instance "+instanceID+" has sent " + cacheReqCountPerInstance[instanceID] + " push DS Msg in response to cross-partition");
//        }
    }

    /** Response of fetch_value, submit instance local state to manager, for partition CC or monitor thread  */
    static public void onAnswerDSMessage(int instanceID, setDSMessage msg) {
        System.out.println("Received instance answer_DS for tuple " + msg.getKey() + ", value: " + msg.getValue());
        MorphStreamEnv.fetchedValues.put(msg.getKey(), msg.getValue());
    }
}
