import libVNFFrontend.NativeInterface;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class OpenNFController implements Runnable {
    //FlowID: (srcIP, destIP, srcPort, destPort, protocol), each flow of packets is mapped to a dedicated instance
//    private HashMap<String, HashMap<String, String[]>> forwardingTable; // VNF ID -> (flow ID -> instance ID under the VNF) because VNFs have different levels of parallelism
//    private HashMap<String, ArrayDeque<String>> packetQueues; // Each VNF has a packet queue
//    private BlockingQueue<String> pktSignalQueue; //which VNF receives a new packet

    private Map<String, Set<String>> instancesWaitingForState; // Instances waiting for state updates


    // SFC: input source -> (controller) -> VNF1 -> (controller) -> VNF2 -> (controller) -> VNF3 -> output
    // libVNF: instance local state storage + read/write state, listen to controller for new packet and state update actions, send processed packet to controller

    //Initialized by libVNF
    public OpenNFController(String[] vnfIDs, HashMap<String, HashMap<String, String[]>> forwardingTable) {
//        this.forwardingTable = forwardingTable;
//        packetQueues = new HashMap<>();
//        for (String vnfID : vnfIDs) {
//            packetQueues.put(vnfID, new ArrayDeque<>());
//        }
//        pktSignalQueue = new LinkedBlockingQueue<>();

        packetQueues = new HashMap<>();
        eventQueue = new ArrayBlockingQueue<>(100);
        sharedStates = new ConcurrentHashMap<>();
        instancesWaitingForState = new ConcurrentHashMap<>();

        for (String vnfID : vnfIDs) {
            packetQueues.put(vnfID, new ArrayBlockingQueue<>(100));
            sharedStates.put(vnfID, new ConcurrentHashMap<>());
            instancesWaitingForState.put(vnfID, new HashSet<>());
        }
    }

    public void sendPktToController(String vnfID, String pkt) {
        packetQueues.get(vnfID).add(pkt);
        // pktSignalQueue.add(vnfID);
    }

    public void registerREvent(String instanceID, String vnfID, String stateKey, String stateValue) {
        Map<String, String> stateUpdate = new HashMap<>();
        stateUpdate.put(stateKey, stateValue);
        sharedStates.get(vnfID).put(instanceID, stateUpdate);
        instancesWaitingForState.get(vnfID).add(instanceID);
        eventQueue.add(instanceID);
    }



    /**
     * Packet format: String -> String.split(",")
     * 0 -> flowID
     * 1 onwards -> stateIDs (optional)
     * 2 onwards -> application-layer payload (optional)
     * */
    public void run() {
//        try {
//            while (true) {
//                String vnfID = pktSignalQueue.take();
//                String pkt = packetQueues.get(vnfID).poll();
//                String flowID = pkt.split(",")[0];
//                for (String instanceID : forwardingTable.get(vnfID).get(flowID)) {
//                    NativeInterface.__process_packet(instanceID, pkt); //TODO: instance, state update action
//                }
//            }
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
        try {
            while (true) {
                String instanceID = eventQueue.take();
                String vnfID = getInstanceVNFID(instanceID);
                String pkt = packetQueues.get(vnfID).poll();
                if (pkt != null) {
                    processPacket(instanceID, pkt);
                }
                updateStates(instanceID, vnfID);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }


        private String getInstanceVNFID(String instanceID) {
            // Extract VNF ID from instance ID (need to implement this logic)
            return instanceID.split("_")[0];
        }

        private void processPacket(String instanceID, String pkt) {
            // Trigger state access events and update shared states as needed
        }

        private void updateStates(String instanceID, String vnfID) {
            // Check if the instance has finished processing and update its state
            instancesWaitingForState.get(vnfID).remove(instanceID);
            if (instancesWaitingForState.get(vnfID).isEmpty()) {
                Map<String, String> updatedState = new HashMap<>();
                for (String waitingInstanceID : sharedStates.get(vnfID).keySet()) {
                    updatedState.putAll(sharedStates.get(vnfID).get(waitingInstanceID));
                }
                // Forward the updated state to all instances
                for (String waitingInstanceID : sharedStates.get(vnfID).keySet()) {
                    sharedStates.get(vnfID).get(waitingInstanceID).putAll(updatedState);
                }
            }
        }


    }

    // TODO: Above is a brief simulation of OpenNFController's functionality on cross-flow state sharing (paper section 5.2.2),
    //  we also need to reproduce its Move() mechanism in paper section 5.1 in the following weeks. Plan well.

}
