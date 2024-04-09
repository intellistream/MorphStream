package intellistream.morphstream.api.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

public class SocketListener implements Runnable {
    private LinkedBlockingQueue<byte[]> messageQueue;
    private ServerSocket serverSocket;

    public SocketListener(LinkedBlockingQueue<byte[]> messageQueue, int port) throws IOException {
        this.messageQueue = messageQueue;
        this.serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (Socket socket = serverSocket.accept()) {
                int senderPort = socket.getPort();
                InputStream input = socket.getInputStream();
                byte[] buffer = new byte[1024]; // Buffer for reading data
                int bytesRead = input.read(buffer); // Read the message into the buffer

                if (bytesRead > 0) {
                    // Convert senderPort to byte array and concatenate with ";" and message
                    // Format: senderPort + ";" + message
                    String senderPortStr = senderPort + ";";
                    byte[] senderPortBytes = senderPortStr.getBytes();
                    byte[] result = new byte[senderPortBytes.length + bytesRead];
                    System.arraycopy(senderPortBytes, 0, result, 0, senderPortBytes.length);
                    System.arraycopy(buffer, 0, result, senderPortBytes.length, bytesRead);

                    System.out.println("Received message from port " + senderPort + ": " + new String(buffer, 0, bytesRead));

                    messageQueue.add(result); // Add the concatenated result to the message queue
                }
            } catch (IOException e) {
                System.out.println("Exception occurred when trying to connect to VM2: " + e.getMessage());
            }
        }
    }
}
