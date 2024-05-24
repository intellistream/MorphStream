package message;

import intellistream.morphstream.api.input.*;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.example.protobuf.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class VNFCtlStubImpl {
    // Callbacks on VNF messages.
    private static ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues = AdaptiveCCManager.tpgQueues;
    private static int tpgReqCount = 0;
    private static final int numSpouts = MorphStreamEnv.get().configuration().getInt("tthread");

    /** Offloading CC, TPG CC, submit txn req to executor */
    static public void onTxnReqMessage(int instanceID, TxnReqMessage msg) {
        if (msg.getCc().getNumber() == 0) { // Partition
            PartitionCCThread.submitPartitionRequest(
                    new PartitionData(System.nanoTime(), msg.getId(), instanceID, msg.getKey(), -1));
            System.out.println("Server received Partition_Req from client: " + msg.getId());

        } else if (msg.getCc().getNumber() == 2) { // Offloading
            int saType = MorphStreamEnv.get().getSaTypeMap().get(msg.getSaIdx());
            OffloadCCThread.submitOffloadReq(
                    new OffloadData(System.nanoTime(), instanceID, msg.getId(), msg.getKey(), 0, msg.getSaIdx(), 0, saType));
            System.out.println("Server received Offloading_Req from client: " + msg.getId());

        } else if (msg.getCc().getNumber() == 3) { // TPG
            tpgQueues.get(tpgReqCount % numSpouts).offer(
                    new TransactionalVNFEvent(-1, instanceID, System.nanoTime(), msg.getId(), msg.getKey(), 0, msg.getSaIdx(), 0));
            System.out.println("Server received TPG_Req from client: " + msg.getId());
            tpgReqCount++;
        }

    }

    /** VNF initialization */
    static public void onSFCJsonMessage(int instanceID, SFCMessage msg) {
        MorphStreamEnv.get().vnfJSON = msg.getSFCJson();
        System.out.println("Server received SFCJson from client: " + msg.getSFCJson());
    }

    /** Monitor pattern report */
    static public void onMonitorReportMessage(int instanceID, MonitorReportMessage msg) {
        MonitorThread.submitPatternData(new PatternData(System.nanoTime(), instanceID, msg.getKey(), msg.getIsWrite()));
        System.out.println("Server received MonitorReport from client: " + msg.getCcValue());
    }

    /** Currently no use */
    static public void onPushCCMessage(int instanceID, setCCMessage msg) {
        System.out.println("Server received PushCC from client: " + msg.getCcValue());
    }

    /** Cache CC, submit state sync to Cache CC executor */
    static public void onPushDSMessage(int instanceID, setDSMessage msg) {
        CacheCCThread.submitReplicationRequest(new CacheData(System.nanoTime(), instanceID, msg.getKey(), msg.getValue()));
        System.out.println("Server received Cache_Req from client: " + msg.getValue());
    }
}
