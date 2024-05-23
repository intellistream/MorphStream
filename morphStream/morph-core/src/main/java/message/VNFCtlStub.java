package message;

import org.example.protobuf.*;

import java.io.IOException;
import java.net.Socket;

public class VNFCtlStub {
    private int instanceID;
    private Socket socket;

    VNFCtlStub(int instanceID, Socket socket) {
        this.instanceID = instanceID;
        this.socket = socket;
    }

    public void make_pause() throws IOException {
        PauseMessage response = PauseMessage.newBuilder().build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setPauseMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    public void make_continue() throws IOException {
        ContinueMessage response = ContinueMessage.newBuilder().build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setContinueMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    public void update_cc(int key, CC cc) throws IOException {
        setCCMessage response = setCCMessage.newBuilder()
                .setCc(cc)
                .setKey(key)
                .build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setSetCCMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    public void fetch_cc(int key) throws IOException {
        getCCMessage response = getCCMessage.newBuilder()
                .setKey(key)
                .build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setGetCCMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    public void update_value(int key, int value) throws IOException {
        setDSMessage response = setDSMessage.newBuilder()
                .setKey(key)
                .setValue(value)
                .build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setSetDSMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    public void fetch_value(int key) throws IOException {
        getDSMessage response = getDSMessage.newBuilder()
                .setKey(key)
                .build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setGetDSMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    public void txn_handle_done(long pktId) throws IOException {
        TxnDoneMessage response = TxnDoneMessage.newBuilder()
                .setId(pktId)
                .build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setTxnDoneMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    public void execute_sa_udf(long pktId, int saIdx, int key, int value) throws IOException {
        UDFReadyMessage response = UDFReadyMessage.newBuilder()
                .setId(pktId)
                .setKey(key)
                .setSaIdx(saIdx)
                .setValue(value)
                .build();
        MessageFromStateManager wrapper = MessageFromStateManager.newBuilder()
                .setUdfMessage(response)
                .build();
        wrapper.writeDelimitedTo(socket.getOutputStream());
    }

    // Your handler function to implement
    public void handleConnection(int instanceID, Socket socket) {
        // Implement your logic here
        while (true) {
            try {
                MessageFromVNFInst wrapper = MessageFromVNFInst.parseDelimitedFrom(socket.getInputStream());
                if (wrapper.hasMonitorReportMessage()) {
                    VNFCtlStubImpl.onMonitorReportMessage(instanceID, wrapper.getMonitorReportMessage());
                } else if (wrapper.hasPushCCMessage()) {
                    VNFCtlStubImpl.onPushCCMessage(instanceID, wrapper.getPushCCMessage());
                } else if (wrapper.hasPushDSMessage()) {
                    VNFCtlStubImpl.onPushDSMessage(instanceID, wrapper.getPushDSMessage());
                } else if (wrapper.hasSfcMessage()) {
                    VNFCtlStubImpl.onSFCJsonMessage(instanceID, wrapper.getSfcMessage());
                } else if (wrapper.hasTxnReqMessage()) {
                    VNFCtlStubImpl.onTxnReqMessage(instanceID, wrapper.getTxnReqMessage());
                } else {
                    System.out.println("Unknown type of request from VNF.");
                }
            } catch (IOException e) {
                System.err.println("Error reading input stream: " + e.getMessage());
            }
        }
    }
}
