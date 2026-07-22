package intellistream.morphstream.api.input;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.Configuration;
import junit.framework.TestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileDataGeneratorTest extends TestCase {
    public FileDataGeneratorTest() {
        super("FileDataGenerator");
    }
    public void testApp() throws IOException {
        Path temporaryDirectory = Files.createTempDirectory("morphstream-generator-test");
        Path inputFile = temporaryDirectory.resolve("events.txt");
        try {
            Configuration configuration = MorphStreamEnv.get().configuration();
            configuration.put("rootPath", temporaryDirectory.toString());
            configuration.put("inputFilePath", inputFile.toString());
            configuration.put("tthread", 1);
            configuration.put("checkpoint", 1);
            configuration.put("totalEvents", 1);
            configuration.put("table1_num_items", 10);
            configuration.put("table2_num_items", 10);

            MorphStreamEnv.get().databaseInitializer().configure_db();
            FileDataGenerator fileDataGenerator = new FileDataGenerator();
            assertEquals(inputFile.toString(), fileDataGenerator.prepareInputData(false));
            assertTrue(Files.size(inputFile) > 0);
        } finally {
            Files.deleteIfExists(inputFile);
            Files.deleteIfExists(temporaryDirectory);
        }
    }
}
