package intellistream.morphstream.api.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;

public class CCInputListener implements Runnable {
    private LinkedBlockingQueue<byte[]> messageQueue;
    private ServerSocket serverSocket;

    public CCInputListener(LinkedBlockingQueue<byte[]> messageQueue, int port) throws IOException {
        this.messageQueue = messageQueue;
        this.serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try (Socket socket = serverSocket.accept()) {

                InputStream input = socket.getInputStream();
                byte[] buffer = new byte[1024]; // Buffer for reading data
                int bytesRead = input.read(buffer); // Read the message into the buffer

                if (bytesRead > 0) {
                    byte[] message = new byte[bytesRead]; // Create an array of the exact length
                    System.arraycopy(buffer, 0, message, 0, bytesRead); // Copy the relevant bytes

                    messageQueue.add(message);
                }

            } catch (IOException e) {
                System.out.println("Exception occurred when trying to connect to VM2: " + e.getMessage());
            }
        }
    }
}
