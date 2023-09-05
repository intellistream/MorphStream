package intellistream.morphstream.api.input;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

public class InputSourceTest extends TestCase {
    public InputSourceTest(String testName) {
        super(testName);
    }
    public static Test suite()
    {
        return new TestSuite( InputSourceTest.class );
    }
    public void testApp() throws IOException {
        assertTrue(true);
        InputSource inputSource = new InputSource(InputSource.InputSourceType.FILE_STRING);
        inputSource.setStaticInputSource("/Users/curryzjj/hair-loss/MorphStream/Benchmark/inputs/events.txt");
        for (int i = 0; i < 100; i++) {
            TransactionalEvent txnEvent = inputSource.getNextTxnEvent();
            System.out.println(txnEvent);
        }
    }
}