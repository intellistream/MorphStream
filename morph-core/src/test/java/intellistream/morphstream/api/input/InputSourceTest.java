package intellistream.morphstream.api.input;

import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class InputSourceTest extends TestCase {
    public InputSourceTest(String testName) {
        super(testName);
    }
    public void testFileInputIsDistributedAcrossSpouts() throws IOException {
        Path inputFile = Files.createTempFile("morphstream-events", ".txt");
        try {
            Files.write(inputFile, Arrays.asList(
                    "accounts:1;amount:10;amount:int;deposit;false",
                    "accounts:2;amount:20;amount:int;deposit;false"
            ));

            InputSource inputSource = new InputSource();
            inputSource.initialize(
                    inputFile.toString(),
                    InputSource.InputSourceType.FILE_STRING,
                    2
            );

            assertEquals(1, inputSource.getInputQueue(0).size());
            assertEquals(1, inputSource.getInputQueue(1).size());
            assertEquals(inputFile.toString(), inputSource.getStaticFilePath());
        } finally {
            Files.deleteIfExists(inputFile);
        }
    }
}
