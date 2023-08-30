package cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.launcher.MorphStreamEvn;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

/**
 * TODO: Implementation of a simple command line frontend for executing programs.
 */
public class CliFrontend {
    private static final Logger LOG = LoggerFactory.getLogger(CliFrontend.class);
    private String appName = "";
    private final MorphStreamEvn env = MorphStreamEvn.get();
    public static CliFrontend getOrCreate() {
        return new CliFrontend();
    }
    public CliFrontend appName(String appName) {
        this.appName = appName;
        return new CliFrontend();
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

    public boolean initializeDB() {
        //TODO:initialize Database
        return true;
    }
    public void run() {

    }

    public void stop() {

    }

    public MorphStreamEvn evn() {
        return env;
    }
}
