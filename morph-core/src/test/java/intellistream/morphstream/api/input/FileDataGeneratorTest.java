package intellistream.morphstream.api.input;

import junit.framework.TestCase;

import java.io.IOException;

public class FileDataGeneratorTest extends TestCase {
    public FileDataGeneratorTest() {
        super("FileDataGenerator");
    }
    public void testApp() throws IOException {
        assertTrue(true);
        FileDataGenerator fileDataGenerator = new FileDataGenerator();
        fileDataGenerator.prepareInputData();
    }
}
