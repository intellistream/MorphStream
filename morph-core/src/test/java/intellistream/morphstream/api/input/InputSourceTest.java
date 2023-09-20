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
        InputSource inputSource = new InputSource();
        inputSource.initialize("/Users/curryzjj/hair-loss/MorphStream/Benchmark/inputs/events.txt", InputSource.InputSourceType.FILE_STRING, 1);
        for (int i = 0; i < 100; i++) {
        }
    }
}
