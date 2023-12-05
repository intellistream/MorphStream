import libVNFFrontend.NativeInterface;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class OpenNFController implements Runnable {
    //FlowID: (srcIP, destIP, srcPort, destPort, protocol), each flow of packets is mapped to a dedicated instance
    private HashMap<String, HashMap<String, String[]>> forwardingTable; // VNF ID -> (flow ID -> instance ID under the VNF) because VNFs have different levels of parallelism
    private HashMap<String, ArrayDeque<String>> packetQueues; // Each VNF has a packet queue
    private BlockingQueue<String> pktSignalQueue; //which VNF receives a new packet

    // SFC: input source -> (controller) -> VNF1 -> (controller) -> VNF2 -> (controller) -> VNF3 -> output
    // libVNF: instance local state storage + read/write state, listen to controller for new packet and state update actions, send processed packet to controller

    //Initialized by libVNF
    public OpenNFController(String[] vnfIDs, HashMap<String, HashMap<String, String[]>> forwardingTable) {
        this.forwardingTable = forwardingTable;
        packetQueues = new HashMap<>();
        for (String vnfID : vnfIDs) {
            packetQueues.put(vnfID, new ArrayDeque<>());
        }
        pktSignalQueue = new LinkedBlockingQueue<>();
    }

    public void sendPktToController(String vnfID, String pkt) {
        packetQueues.get(vnfID).add(pkt);
        pktSignalQueue.add(vnfID);
    }

    /**
     * Packet format: String -> String.split(",")
     * 0 -> flowID
     * 1 onwards -> stateIDs (optional)
     * 2 onwards -> application-layer payload (optional)
     * */
    public void run() {
        try {
            while (true) {
                String vnfID = pktSignalQueue.take();
                String pkt = packetQueues.get(vnfID).poll();
                String flowID = pkt.split(",")[0];
                for (String instanceID : forwardingTable.get(vnfID).get(flowID)) {
                    NativeInterface.__process_packet(instanceID, pkt); //TODO: instance, state update action
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // TODO: Above is a brief simulation of OpenNFController's functionality on cross-flow state sharing (paper section 5.2.2),
    //  we also need to reproduce its Move() mechanism in paper section 5.1 in the following weeks. Plan well.

}
