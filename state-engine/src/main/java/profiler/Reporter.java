package profiler;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Reporter {
    public static boolean closed = false;
    private static DatagramSocket socket;
    private static InetSocketAddress address;
    public static Integer tthread;

    /**
     * Open Socket
     * @param host: host of the server
     * @param port: port number
     */
    public void open(String host, Integer port) {
        if (host == null || host.isEmpty() || port < 1) {
            // illegal argument => throw Exception
            throw new IllegalArgumentException(
                    "Invalid host/port argument. Host: " + host + " Port: " + port
            );
        } else {
            address = new InetSocketAddress(host, port);

            try {
                socket = new DatagramSocket(0);
            } catch (SocketException e) {
                throw new RuntimeException("Could not create new datagram socket.", e);
            }
        }
    }

    /**
     * Close the socket
     */
    public void close() {
        closed = true;
        if (socket != null && !socket.isClosed()) {
            socket.close();
            log.info("Reporter socket closed.");
        }
    }

    /**
     * Initialize Reporter
     */
    public static void Initialize(Integer tthread) {
        Reporter.tthread = tthread;
    }

    /**
     * Report performance information using the existing socket
     */
    public void report() {
        reportLatency();
        reportThroughput();
    }

    public void reportLatency() {
        List<Double> latency = new ArrayList<>();
        for (int i = 0; i < tthread; i++) {
            latency.add(Metrics.RuntimePerformance.Latency[i].getMean());
        }
        // get the average of all threads' latency
        double overallLatency = latency.stream().mapToDouble(d -> d).average().orElse(0.0);
        send("latency", overallLatency);
    }

    public void reportThroughput() {
        List<Double> throughput = new ArrayList<>();
        for (int i = 0; i < tthread; i++) {
            throughput.add(Metrics.RuntimePerformance.Throughput[i].getMean());
        }
        // get sum of all threads' throughput
        double overallThroughput = throughput.stream().mapToDouble(d -> d).sum();
        send("throughput", overallThroughput);
    }

    private void send(String name, double value) {
        send(name, String.valueOf(value));
    }

    private void send(String name, long value) {
        send(name, String.valueOf(value));
    }

    /**
     * Send message through Socket
     */
    private void send(String name, String value) {
        try {
            String formatted = String.format("%s:%s", name, value);
            byte[] data = formatted.getBytes(StandardCharsets.UTF_8);
            socket.send(new DatagramPacket(data, data.length, address));
        } catch (IOException e) {
            log.error("unable to send packet to: {}:{}", address.getHostName(), address.getPort());
        }
    }
}
