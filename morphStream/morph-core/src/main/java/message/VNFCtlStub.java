package message;

import org.example.protobuf.*;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

public class VNFCtlStub {
    private int instanceID;
    private Socket socket;
    private int reqCount = 0;

    VNFCtlStub(int instanceID, Socket socket) {
        this.instanceID = instanceID;
        this.socket = socket;
        VNFCtlStubImpl.initializeVNFCtrlStubImpl();
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

    public void handleConnection(int instanceID, Socket socket) {
        try {
            // Set socket timeout if desired
//            socket.setSoTimeout(5000); // 5 seconds timeout for example
            while (!socket.isClosed()) {
                try {
                    MessageFromVNFInst wrapper = MessageFromVNFInst.parseDelimitedFrom(socket.getInputStream());
                    if (wrapper == null) {
                        // Handle the null case appropriately
                        System.out.println("Received null wrapper. Possibly end of stream.");
                        break; // or continue; depending on your logic
                    }
                    reqCount++;
                    if (wrapper.hasMonitorReportMessage()) {
                        VNFCtlStubImpl.onMonitorReportMessage(instanceID, wrapper.getMonitorReportMessage());
                    } else if (wrapper.hasAnswerCCMessage()) {
                        VNFCtlStubImpl.onPushCCMessage(instanceID, wrapper.getAnswerCCMessage());
                    } else if (wrapper.hasPushDSMessage() && wrapper.getMsgCase().equals(MessageFromVNFInst.MsgCase.PUSHDSMESSAGE)) {
                        VNFCtlStubImpl.onPushDSMessage(instanceID, wrapper.getPushDSMessage());
                    } else if (wrapper.hasPushDSMessage() && wrapper.getMsgCase().equals(MessageFromVNFInst.MsgCase.ANSWERDSMESSAGE)) {
                        VNFCtlStubImpl.onAnswerDSMessage(instanceID, wrapper.getPushDSMessage());
                    } else if (wrapper.hasSfcMessage()) {
                        VNFCtlStubImpl.onSFCJsonMessage(instanceID, wrapper.getSfcMessage());
                    } else if (wrapper.hasTxnReqMessage()) {
                        VNFCtlStubImpl.onTxnReqMessage(instanceID, wrapper.getTxnReqMessage());
                    } else {
                        System.out.println("Unknown type of request from VNF.");
                    }
                } catch (SocketException e) {
                    if (e.getMessage().contains("Socket closed")) {
                        System.err.println("Connection closed by the client.");
                    } else {
                        System.err.println("Socket error: " + e.getMessage());
                    }
                    break;
                } catch (IOException e) {
                    System.err.println("Error reading input stream: " + e.getMessage());
                    // Additional handling can be added here, like breaking the loop or retrying
                } catch (NullPointerException e) {
                    System.err.println("Null pointer exception: " + e.getMessage());
                }
            }
        }
        finally {
            if (!socket.isClosed()) {
//                System.out.println("Socket NOT closed by server yet. Total request: " + reqCount);
//                    socket.close();
                System.out.println("Socket is still running. Total request: " + reqCount);
            } else {
                System.out.println("Socket closed by client. Total request: " + reqCount);
            }

        }
    }
}
