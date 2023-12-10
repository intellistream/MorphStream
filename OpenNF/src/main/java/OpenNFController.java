import libVNFFrontend.NativeInterface;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class OpenNFController implements Runnable {
    //FlowID: (srcIP, destIP, srcPort, destPort, protocol), each flow of packets is mapped to a dedicated instance
    //private HashMap<String, HashMap<String, String[]>> forwardingTable; // VNF ID -> (flow ID -> instance ID under the VNF) because VNFs have different levels of parallelism
    private HashMap<String, ArrayBlockingQueue<String>> packetQueues; // Each VNF has a packet queue
//    private BlockingQueue<String> pktSignalQueue; //which VNF receives a new packet

    private Map<String, Set<String>> instancesWaitingForState; // Instances waiting for state updates


    private BlockingQueue<String> eventQueue; // A collection of registered registrationEvents (REvent)

    private Map<String, Map<String, String>> sharedStates; // key is VNF ID, value is a map of instance ID -> state update
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

        packetQueues = new HashMap<>(); // key is VNF ID, value is a packet queue
        eventQueue = new ArrayBlockingQueue<>(100); // A collection of registered registrationEvents (REvent)
        sharedStates = new ConcurrentHashMap<>(); // key is VNF ID, value is a map of instance ID -> state update
        instancesWaitingForState = new ConcurrentHashMap<>(); // key is VNF ID, value is a set of instance IDs, which are waiting for state updates

        for (String vnfID : vnfIDs) {
            // initialize packet queue, shared state map, and instance waiting set for each VNF
            packetQueues.put(vnfID, new ArrayBlockingQueue<>(100));
            sharedStates.put(vnfID, new ConcurrentHashMap<>());
            instancesWaitingForState.put(vnfID, new HashSet<>());
        }
    }

    public void sendPktToController(String vnfID, String pkt) {
        packetQueues.get(vnfID).add(pkt);
        // pktSignalQueue.add(vnfID);
    }

    /**
     *  方法：这个方法用于注册共享状态访问事件，
     *  接受实例ID、VNF ID、状态键（stateKey）和状态值（stateValue）作为参数。
     *  当实例需要访问或更新共享状态时，它会调用这个方法，
     *  将访问状态的请求作为一个事件注册到控制器（OpenNFController）中。
     */
    public void registerREvent(String instanceID, String vnfID, String stateKey, String stateValue) {
        Map<String, String> stateUpdate = new HashMap<>();
        stateUpdate.put(stateKey, stateValue);
        sharedStates.get(vnfID).put(instanceID, stateUpdate.get(vnfID));
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
                // TODO: Block until the instance finishes processing the packet and sends back a SIGNAL.
                //  After controller receives the signal, it use getState() to retrieve the state update results from the instance,
                //  and then broadcast this state update action to all corresponding instances using putState().
                //
                updateStates(instanceID, vnfID); //TODO: Notify all corresponding instances that share the state, retrieve state sharing information from a table.
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }
    private String getInstanceVNFID(String instanceID) {
        // Extract VNF ID from instance ID (need to implement this logic)
        return instanceID.split("_")[0];
    }

    private void processPacket(String instanceID, String pkt) {
        // Trigger state access events and update shared states as needed
    }
    private void updateStates(String instanceID, String vnfID) {
        // Update shared states for instances waiting for state updates
        }
    }

    // TODO: Above is a brief simulation of OpenNFController's functionality on cross-flow state sharing (paper section 5.2.2),
    //  we also need to reproduce its Move() mechanism in paper section 5.1 in the following weeks. Plan well.



