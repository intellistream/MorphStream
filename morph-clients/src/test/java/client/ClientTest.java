package client;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.zeromq.ZMQ;
import worker.MorphStreamWorkerTest;

public class ClientTest extends TestCase {
    public ClientTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(MorphStreamWorkerTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        assertTrue(true);
        try (ZMQ.Context context = ZMQ.context(1);
             ZMQ.Socket socket = context.socket(ZMQ.REQ)) {
            socket.connect("tcp://localhost:5555");
            for (int i = 0; i < 10; i++) {
                byte[] request = String.valueOf(i).getBytes();
                socket.send(request, 0);
                System.out.println("Send：" + i);
            }
        }
    }
}
