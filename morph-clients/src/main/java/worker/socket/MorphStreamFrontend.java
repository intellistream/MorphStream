package worker.socket;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.*;

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

        String[] workerHosts = MorphStreamEnv.get().configuration().getString("morphstream.socket.workerHosts").split(",");
        String[] workerPorts = MorphStreamEnv.get().configuration().getString("morphstream.socket.workerPorts").split(",");
        poller = zContext.createPoller(workerPorts.length);
        for (int i = 0; i < workerHosts.length; i++) {
            ZMQ.Socket toWorker = zContext.createSocket(SocketType.DEALER);
            toWorker.connect("tcp://" + workerHosts[i] + ":" + workerPorts[i]);
            sockets.put(workerHosts[i] + ":" + workerPorts[i], toWorker);
            poller.register(toWorker, ZMQ.Poller.POLLIN);
        }
    }
    public void asyncReceiveFunctionOutput() {
        for (int centitick = 0; centitick < 100; centitick++) {
            poller.poll(0);
            for (int i = 0; i < sockets.size(); i ++) {
                if (poller.pollin(i)) {
                    ZMsg msg = ZMsg.recvMsg(getSocketFormIndex(i));
                    LOG.info("Receive: " + msg.popString());
                    msg.destroy();
                }
            }
        }
    }
    public void invokeFunctionToWorker(ZMQ.Socket address, ZMsg msg) {
        address.send(msg.getLast().toString());
    }

    public void run(){
        while (!Thread.currentThread().interrupted()) {
            ZMsg msg = ZMsg.recvMsg(frontend, false);
            if (msg != null) {
                invokeFunctionToWorker(getSocketRandom(), msg);
            }
            asyncReceiveFunctionOutput();
        }
    }
    private ZMQ.Socket getSocketRandom() {
        List<String> keyList = new ArrayList<>(sockets.keySet());
        Random random = new Random();
        int randomIndex = random.nextInt(keyList.size());
        return sockets.get(keyList.get(randomIndex));
    }
    private ZMQ.Socket getSocketFormIndex(int index) {
        List<String> keyList = new ArrayList<>(sockets.keySet());
        return sockets.get(keyList.get(index));
    }
}
