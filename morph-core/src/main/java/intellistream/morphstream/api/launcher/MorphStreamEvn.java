package intellistream.morphstream.api.launcher;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.optimization.OptimizationManager;
import intellistream.morphstream.engine.stream.topology.TransactionTopology;
import intellistream.morphstream.engine.txn.db.Database;

import java.util.HashMap;

public class MorphStreamEvn {
    private JCommanderHandler jCommanderHandler;
    private Configuration configuration;
    private Database database;
    private OptimizationManager OM;
    private TransactionTopology topology;
    public static MorphStreamEvn ourInstance = new MorphStreamEvn();
    public static MorphStreamEvn get() {
        return ourInstance;
    }
    public Configuration configuration() {
        return configuration;
    }
    public JCommanderHandler jCommanderHandler() {
        return jCommanderHandler;
    }
    public Database database() {
        return database;
    }
    public OptimizationManager OM() {
        return OM;
    }

    public TransactionTopology topology() {
        return topology;
    }

}
