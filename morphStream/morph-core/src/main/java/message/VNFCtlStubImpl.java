package message;

import org.example.protobuf.*;

public class VNFCtlStubImpl {
    // Callbacks on VNF messages.
    static public void onTxnReqMessage(TxnReqMessage msg) {
        System.out.println("Received message from client: " + msg.getId());
    }
    static public void onSFCJsonMessage(SFCMessage msg) {
        System.out.println("Received message from client: " + msg.getSFCJson());
    }
    static public void onMonitorReportMessage(MonitorReportMessage msg) {
        System.out.println("Received message from client: " + msg.getCcValue());
    }

    static public void onPushCCMessage(setCCMessage msg) {
        System.out.println("Received message from client: " + msg.getCcValue());
    }
    static public void onPushDSMessage(setDSMessage msg) {
        System.out.println("Received message from client: " + msg.getValue());
    }
}
