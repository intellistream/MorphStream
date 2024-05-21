package message;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class VNFCtrlServer {

    public VNFCtlStub[] ListenForInstances(int port, int VNF_Cnt) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port);
        VNFCtlStub[] vnfStubs = new VNFCtlStub[VNF_Cnt];
        for (int i = 0; i < VNF_Cnt; i++){
            Socket connection = serverSocket.accept();
            System.out.println("Accepted connection from: " + connection.getInetAddress());

            vnfStubs[i] = new VNFCtlStub(i, connection);

            final VNFCtlStub cur = vnfStubs[i];
            final int index = i;
            Thread thread = new Thread(() -> {
                // Call your handler function here, passing in the index and the socket
                // connection
                cur.handleConnection(index, connection); //TODO
            });
            thread.start();
        }
        serverSocket.close();
        return vnfStubs;
    }
}
