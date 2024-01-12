import client.ClientTest;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import worker.DriverTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Thread.sleep;

public class BenchmarkTest {
    /**
     * Rigourous Test :-)
     */
    public static void main(String[] args) {
        try {
            MorphStreamEnv.get().LoadConfiguration(null, args); //TODO: add loadConfig from file
            DriverTest driverTest = new DriverTest();
            driverTest.initialize();
            driverTest.start();
            List<ClientTest> threads = new ArrayList<>();
            int clientNum = MorphStreamEnv.get().configuration().getInt("clientNum");
            for (int threadNum = 0; threadNum < clientNum; threadNum++) {
                ClientTest t = new ClientTest(threadNum);
                t.initialize(threadNum, MorphStreamEnv.get().clientLatch());
                threads.add(t);
                t.start();
            }
            sleep(205000);
            driverTest.stopDriver();
            for (int threadNum = 0; threadNum < clientNum; threadNum++) {
                threads.get(threadNum).stopRunning();
            }
            driverTest.display();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
