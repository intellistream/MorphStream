package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private String appName = "";
    private final MorphStreamEnv env = MorphStreamEnv.get();
    public static CliFrontend getOrCreate() {
        return new CliFrontend();
    }
    public CliFrontend appName(String appName) {
        this.appName = appName;
        return this;
    }
    public boolean LoadConfiguration(String configPath, String[] args) throws IOException {
        if (configPath != null) {
            env.jCommanderHandler().loadProperties(configPath);
        }
        JCommander cmd = new JCommander(env.jCommanderHandler());
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            if (enable_log) LOG.error("Argument error: " + ex.getMessage());
            cmd.usage();
            return false;
        }
        return true;
    }

    public void prepare() throws IOException {
        //TODO:initialize Database and configure input and output
        env.databaseInitialize().creates_Table();
        String inputFile = env.configuration().getString("input.file");
        File file = new File(inputFile);
        if (file.exists()) {
            LOG.info("Data already exists.. skipping data generation...");
        } else {
            String fileName = env.fileDataGenerator().prepareInputData();
            env.configuration().put("input.file", fileName);
        }
    }
    public void run() {

    }

    public void stop() {

    }

    public MorphStreamEnv evn() {
        return env;
    }
}
