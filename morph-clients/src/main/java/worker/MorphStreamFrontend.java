package worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

public class MorphStreamFrontend extends Thread{
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamFrontend.class);
    private ZMQ.Socket frontend;// Frontend socket talks to clients over TCP
    private ZContext zContext;
    public MorphStreamFrontend(ZContext zContext) {
        this.zContext = zContext;
        this.frontend = zContext.createSocket(SocketType.DEALER);
        frontend.connect("inproc://backend");
    }

    public void run(){
        while (!Thread.interrupted()) {
            ZMsg msg = ZMsg.recvMsg(frontend);
            LOG.info("Received message from client: {}", msg);
        }
    }
}
