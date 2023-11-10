package client;

import org.zeromq.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class asyncsrv {
    //This is our client task
    //It connects to the server, and then sends a request once per second
    //It collects responses as they arrive, and it prints them out. We will
    //run several client tasks in parallel, each with a different random ID.

    private static final Random rand = new Random(System.nanoTime());

    private static class client_task implements Runnable {

        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                ZMQ.Socket client = ctx.createSocket(SocketType.DEALER);

                //  Set random identity to make tracing easier
                String identity = String.format("%04X-%04X", rand.nextInt(), rand.nextInt());
                client.setIdentity(identity.getBytes(ZMQ.CHARSET));
                client.connect("tcp://localhost:5570");

                ZMQ.Poller poller = ctx.createPoller(1);
                poller.register(client, ZMQ.Poller.POLLIN);

                int requestNbr = 0;
                while (!Thread.currentThread().isInterrupted()) {
                    //Tick once per second, pulling in arriving messages
                    for (int centitick = 0; centitick < 100; centitick++) {
                        poller.poll(10);
                        if (poller.pollin(0)) {
                            ZMsg msg = ZMsg.recvMsg(client);
                            msg.getLast().print(identity);
                            msg.destroy();
                        }
                    }
                    client.send(String.format(identity), 0);
                }
            }
        }
    }

    //This is our server task.
    //It uses the multithreaded server model to deal requests out to a pool of workers and route replies back to clients.
    //One worker can handle one request at a time but one client can talk to multiple workers at once.

    private static class server_task implements Runnable {
        @Override
        public void run() {
            try (ZContext ctx = new ZContext()) {
                ZMQ.Socket server = ctx.createSocket(SocketType.DEALER);
                server.bind("tcp://*:5570");
                int marker = 0;
                List<Integer> buffer = new ArrayList<>();
                while (!Thread.currentThread().isInterrupted()) {
                    byte[] request = server.recv();
                    String text = new String(request);
                    server.send(text);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new Thread(new client_task()).start();
        new Thread(new client_task()).start();
        new Thread(new client_task()).start();
        new Thread(new server_task()).start();

        //  Run for 5 seconds then quit
        Thread.sleep(5 * 1000);
    }
}
