package client;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.zeromq.ZMQ;
import worker.MorphStreamWorkerTest;

public class ServerTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ServerTest(String testName) {
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
             ZMQ.Socket socket = context.socket(ZMQ.REP)) {
            // 绑定端口
            socket.bind("tcp://*:5555");
            while (true) {
                byte[] request = socket.recv();
                String text = new String(request);
                System.out.println("接收到消息: " + text);
            }
        }
    }
}
