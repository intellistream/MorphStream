package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class SocketListener implements Runnable { //A single thread that listens for incoming messages from other VMs through a single socket
    private static LinkedBlockingQueue<PatternData> monitorQueue;
    private static LinkedBlockingQueue<PartitionData> partitionQueue;
    private static LinkedBlockingQueue<CacheData> cacheQueue;
    private static LinkedBlockingQueue<OffloadData> offloadQueue;
    private static ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues; //round-robin input queues for each executor (combo/bolt)
    private static final int tpgThreadNum = MorphStreamEnv.get().configuration().getInt("tthread");
    private static final byte fullSeparator = 59;
    private static final byte msgSeparator = 10;
    private static final int PORT = 8080;
    private final ServerSocket serverSocket = MorphStreamEnv.get().stateManagerSocket();
    private static final int socketHandlerThreadNum = 4; //TODO: Hardcoded


    public SocketListener(LinkedBlockingQueue<PatternData> monitorQueue,
                          LinkedBlockingQueue<PartitionData> partitionQueue,
                          LinkedBlockingQueue<CacheData> cacheQueue,
                          LinkedBlockingQueue<OffloadData> offloadQueue,
                          ConcurrentHashMap<Integer, BlockingQueue<TransactionalEvent>> tpgQueues) {
        SocketListener.monitorQueue = monitorQueue;
        SocketListener.partitionQueue = partitionQueue;
        SocketListener.cacheQueue = cacheQueue;
        SocketListener.offloadQueue = offloadQueue;
        SocketListener.tpgQueues = tpgQueues;
    }


    public void run() {
        while (!Thread.currentThread().isInterrupted()) {

            System.out.println("Server is listening on port " + PORT);
            ExecutorService executorService = Executors.newFixedThreadPool(socketHandlerThreadNum);

            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    int instanceID = clientSocket.getPort();
                    System.out.println("New client connected: " + clientSocket.getRemoteSocketAddress());

                    MorphStreamEnv.get().addInstanceSocket(instanceID, clientSocket);
                    executorService.submit(new ClientHandler(clientSocket, instanceID));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final int instanceID;
        private int rrIndex = 0;
        private int requestCounter = 0;

        public ClientHandler(Socket socket, int instanceID) {
            this.clientSocket = socket;
            this.instanceID = instanceID;
        }

        @Override
        public void run() {
            try (InputStream input = clientSocket.getInputStream()) {
                // Buffer to hold the read data
                List<Byte> messageBuffer = new ArrayList<>();
                int readByte;

                while (true) {  // Changed from while ((readByte = input.read()) != -1)
                    try {
                        if ((readByte = input.read()) != -1) {
                            if (readByte == msgSeparator) {
                                byte[] message = new byte[messageBuffer.size()];
                                for (int i = 0; i < message.length; i++) {
                                    message[i] = messageBuffer.get(i);
                                }
//                                System.out.println("Received from " + clientSocket.getRemoteSocketAddress() + ": " + new String(message));

                                if (message.length == 1) {
                                    System.out.println("Error: reading invalid message: " + message[0]);
                                    System.out.println("Total requests received: " + requestCounter);
                                } else {
                                    List<byte[]> splitByteArrays = splitByteArray(message, fullSeparator);
                                    int target = decodeInt(splitByteArrays.get(0), 0);

                                    if (target == 0) {
                                        monitorQueue.add(byteToPatternData(instanceID, splitByteArrays));
                                    } else if (target == 1) {
                                        partitionQueue.add(byteToPartitionData(instanceID, splitByteArrays));
                                    } else if (target == 2) {
                                        cacheQueue.add(byteToCacheData(instanceID, splitByteArrays));
                                    } else if (target == 3) {
                                        offloadQueue.add(byteToOffloadData(instanceID, splitByteArrays));
                                    } else if (target == 4) {
                                        tpgQueues.get(rrIndex).add(byteToTPGData(instanceID, splitByteArrays));
                                        rrIndex = (rrIndex + 1) % tpgThreadNum;
                                    }
                                    // Clear the buffer for the next message
                                    messageBuffer.clear();
                                    requestCounter++;
                                }

                            } else {
                                messageBuffer.add((byte) readByte);
                            }
                        } else {
                            // -1 returned from read indicates client has closed the connection properly
                            System.out.println("Client has closed the connection: " + clientSocket.getRemoteSocketAddress());
                            System.out.println("Total request received: " + requestCounter);
                            break;
                        }
                    } catch (SocketException se) {
                        // Specific handling for socket related exceptions
                        System.out.println("Socket exception (likely connection reset by client): " + se.getMessage());
                        System.out.println("Total request received: " + requestCounter);
                        break;
                    } catch (IOException ie) {
                        // General I/O exceptions
                        System.out.println("IOException during communication: " + ie.getMessage());
                        System.out.println("Total request received: " + requestCounter);
                        break;
                    }
                }
            } catch (Exception e) {
                System.out.println("Exception when setting up input stream: " + e.getMessage());
                System.out.println("Exception type: " + e.getClass().getName());
                e.printStackTrace();
                System.out.println("Total request received: " + requestCounter);
            }
            finally {
                try {
                    clientSocket.close();
                    System.out.println("Closed client socket.");
                    System.out.println("Total request received: " + requestCounter);
                } catch (IOException e) {
                    System.out.println("Error closing client socket: " + e.getMessage());
                    System.out.println("Total request received: " + requestCounter);
                }
            }
        }
    }


    private static PatternData byteToPatternData(int instanceID, List<byte[]> splitByteArrays) {
        int tupleID = decodeInt(splitByteArrays.get(1), 0);
        boolean value = decodeBoolean(splitByteArrays.get(2), 0);
        return new PatternData(0, instanceID, tupleID, value);
    }

    private static PartitionData byteToPartitionData(int instanceID, List<byte[]> splitByteArrays) {
        int tupleID = decodeInt(splitByteArrays.get(1), 0);
        int value = decodeInt(splitByteArrays.get(2), 0);
        return new PartitionData(-1,-1,instanceID, tupleID, value);
    }

    private static CacheData byteToCacheData(int instanceID, List<byte[]> splitByteArrays) {
        int tupleID = decodeInt(splitByteArrays.get(1), 0);
        int value = decodeInt(splitByteArrays.get(2), 0);
        return new CacheData(0, instanceID, tupleID, value);
    }

    private static OffloadData byteToOffloadData(int instanceID, List<byte[]> splitByteArrays) {
        long timestamp = decodeLong(splitByteArrays.get(1), 0);
        long txnReqID = decodeLong(splitByteArrays.get(2), 0);
        int tupleID = decodeInt(splitByteArrays.get(3), 0);
        int txnIndex = decodeInt(splitByteArrays.get(4), 0);
        int saIndex = decodeInt(splitByteArrays.get(5), 0);
        int isAbort = decodeInt(splitByteArrays.get(6), 0);
        return new OffloadData(timestamp, instanceID, txnReqID, tupleID, txnIndex, saIndex, isAbort, -1);
    }

    private static TransactionalEvent byteToTPGData(int instanceID, List<byte[]> splitByteArrays) {
        long timestamp = decodeLong(splitByteArrays.get(1), 0);
        long txnReqID = decodeLong(splitByteArrays.get(2), 0);
        int tupleID = decodeInt(splitByteArrays.get(3), 0);
        int txnIndex = decodeInt(splitByteArrays.get(4), 0);
        int saIndex = decodeInt(splitByteArrays.get(5), 0);
        int isAbort = decodeInt(splitByteArrays.get(6), 0);
        return new TransactionalVNFEvent(-1, instanceID, timestamp, txnReqID, tupleID, txnIndex, saIndex, isAbort);
    }


    private static long decodeLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (bytes[offset + i] & 0xFF)) << (i * 8);
        }
        return value;
    }

//    private static int decodeInt(byte[] bytes, int offset) {
//        int value = 0;
//        for (int i = 0; i < 4; i++) {
//            value |= (bytes[offset + i] & 0xFF) << (i * 8);
//        }
//        return value;
//    }

    private static int decodeInt(byte[] bytes, int offset) {
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array is null");
        }
        if (offset < 0 || offset + 4 > bytes.length) {
            throw new IllegalArgumentException("Offset and length must be valid for the given byte array - offset: " + offset + ", array length: " + bytes.length);
        }

        int value = 0;
        try {
            for (int i = 0; i < 4; i++) {
                value |= (bytes[offset + i] & 0xFF) << (i * 8);
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            // This should never happen due to the checks above
            System.err.println("Unexpected ArrayIndexOutOfBoundsException: " + e.getMessage());
            throw new IllegalArgumentException("Array index out of bound caught, this should not happen with proper offset and length checks.", e);
        }
        return value;
    }


    private static boolean decodeBoolean(byte[] bytes, int offset) {
        return bytes[offset] != 0;
    }

    private static List<byte[]> splitByteArray(byte[] byteArray, byte separator) {
        List<byte[]> splitByteArrays = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();

        for (int i = 0; i < byteArray.length; i++) {
            if (byteArray[i] == separator) {
                indexes.add(i);
            }
        }

        int startIndex = 0;
        for (Integer index : indexes) {
            byte[] subArray = new byte[index - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, index - startIndex);
            splitByteArrays.add(subArray);
            startIndex = index + 1;
        }

        // Handling the remaining part after the last occurrence of 59
        if (startIndex < byteArray.length) {
            byte[] subArray = new byte[byteArray.length - startIndex];
            System.arraycopy(byteArray, startIndex, subArray, 0, byteArray.length - startIndex);
            splitByteArrays.add(subArray);
        }

        return splitByteArrays;
    }
}
