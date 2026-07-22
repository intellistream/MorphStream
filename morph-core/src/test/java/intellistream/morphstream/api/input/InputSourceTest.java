package intellistream.morphstream.api.input;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class InputSourceTest extends TestCase {
    public InputSourceTest(String testName) {
        super(testName);
    }
    public static Test suite()
    {
        return new TestSuite( InputSourceTest.class );
    }
    public void testApp() throws IOException {
        Path inputFile = Files.createTempFile("morphstream-input-test", ".txt");
        try {
            Files.write(inputFile,
                    Collections.singletonList("table1:1;value:10;value:int;event1;false"),
                    StandardCharsets.UTF_8);

            InputSource inputSource = new InputSource();
            inputSource.initialize(inputFile.toString(), InputSource.InputSourceType.FILE_STRING, 4);
            assertEquals(1, inputSource.getInputQueue(0).size());
            assertEquals("event1", inputSource.getInputQueue(0).peek().getFlag());
        } finally {
            Files.deleteIfExists(inputFile);
        }
    }
}
