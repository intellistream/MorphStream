package message;

import intellistream.morphstream.transNFV.AdaptiveCCManager;
import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class VNFCtrlServer {

    public VNFCtlStub[] listenForInstances(int port, int parallelism) throws IOException {
        System.out.println("VNF Ctrl Server started on port " + port);
        ServerSocket serverSocket = new ServerSocket(port);
        VNFCtlStub[] vnfStubs = new VNFCtlStub[parallelism];
        for (int i = 0; i < parallelism; i++){
            System.out.println("Waiting for connection " + i);
            Socket socketConnection = serverSocket.accept();
            System.out.println("Server accepted connection from: " + socketConnection.getInetAddress());

            vnfStubs[i] = new VNFCtlStub(i, socketConnection);

            final VNFCtlStub instanceStub = vnfStubs[i];
            final int instanceID = i;
            Thread thread = new Thread(() -> {
                // Call your handler function here, passing in the instanceID and the socket
                // connection
                AdaptiveCCManager.vnfStubs.put(instanceID, instanceStub);
                MorphStreamEnv.instanceLocks.put(instanceID, new Object());
                instanceStub.handleConnection(instanceID, socketConnection);
            });
            thread.start();
        }
        serverSocket.close();
        return vnfStubs;
    }
}
