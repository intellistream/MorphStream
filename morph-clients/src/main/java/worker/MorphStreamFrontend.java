package worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.HashMap;
import java.util.Map;

public class MorphStreamFrontend extends Thread{
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamFrontend.class);
    private final Map<String, ZMQ.Socket> sockets = new HashMap<>();
    protected ZMQ.Poller poller;
    private ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    protected int msgCount = 0;
    private ZContext zContext;
    public MorphStreamFrontend(ZContext zContext) {
        this.zContext = zContext;
        this.frontend = zContext.createSocket(SocketType.DEALER);
        frontend.connect("inproc://backend");

        ZMQ.Socket toWorker = zContext.createSocket(SocketType.DEALER);
        toWorker.connect("tcp://" + "localhost" + ":" + 5555);
        sockets.put("localhost", toWorker);
        poller = zContext.createPoller(1);
        poller.register(sockets.get("localhost"), ZMQ.Poller.POLLIN);
    }
    public void asyncReceiveFunctionOutput(String workerName) {
        for (int centitick = 0; centitick < 100; centitick++) {
            poller.poll(0);
            if (poller.pollin(0)) {
                ZMsg msg = ZMsg.recvMsg(sockets.get(workerName));
                LOG.info("Receive: " + msg.popString());
                msg.destroy();
            }
        }
    }
    public void invokeFunctionToWorker(String address, ZMsg msg) {
        sockets.get(address).send(msg.getLast().toString());
    }

    public void run(){
        while (!Thread.currentThread().interrupted()) {
            ZMsg msg = ZMsg.recvMsg(frontend, false);
            if (msg != null) {
                invokeFunctionToWorker("localhost", msg);
            }
            asyncReceiveFunctionOutput("localhost");
        }
    }
}
