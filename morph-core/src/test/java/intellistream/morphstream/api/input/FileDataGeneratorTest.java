package intellistream.morphstream.api.input;

import junit.framework.TestCase;

public class FileDataGeneratorTest extends TestCase {
    public FileDataGeneratorTest() {
        super("FileDataGenerator");
    }
    public void testCanConstructGeneratorWithoutRuntimeConfiguration() {
        FileDataGenerator fileDataGenerator = new FileDataGenerator();
        assertNotNull(fileDataGenerator);
    }
}
