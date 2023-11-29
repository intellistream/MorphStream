import libVNFFrontend.NativeInterface;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class OpenNFController implements Runnable {
    //TODO: OpenNF controller should stay alive, waiting for packets.

    private HashMap<String, String[]> forwardingTable; // Synchronized with input source, maps packet's flowID to the instanceIDs of all instances sharing that flow

    private ArrayDeque<String[]> eventQueue; // Paper Section 5.2.2 FIFO queue, receive packets directly from input source, datatype not sure

    public void sendPktToController(String[] pkt) {
        eventQueue.add(pkt);
    }

    public void run() {
        while (true) {
            if (eventQueue.isEmpty()) {
                continue;
            }
            String[] pkt = eventQueue.poll();
            String flowID = pkt[0];
            for (String instanceID : forwardingTable.get(flowID)) {
                //TODO: extract state update actions from packet and call corresponding packets to execute.
            }
        }
    }

    // TODO: Above is a brief simulation of OpenNFController's functionality on cross-flow state sharing (paper section 5.2.2),
    //  we also need to reproduce its Move() mechanism in paper section 5.1 in the following weeks. Plan well.

}
