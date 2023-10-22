package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import junit.framework.TestCase;

import java.io.IOException;

public class FileDataGeneratorTest extends TestCase {
    public FileDataGeneratorTest() {
        super("FileDataGenerator");
    }
    public void testApp() throws IOException {
        assertTrue(true);
        MorphStreamEnv.get().databaseInitializer().configure_db();
        FileDataGenerator fileDataGenerator = new FileDataGenerator();
        fileDataGenerator.prepareInputData(false);
    }
}
