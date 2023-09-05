package intellistream.morphstream.api.launcher;

import intellistream.morphstream.api.input.FileDataGenerator;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Id.BlockManagerId;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.optimization.OptimizationManager;
import intellistream.morphstream.engine.stream.topology.TransactionTopology;
import intellistream.morphstream.engine.txn.db.Database;

public class MorphStreamEnv {
    private JCommanderHandler jCommanderHandler;
    private Configuration configuration;
    private Database database;
    private OptimizationManager OM;
    private RdmaShuffleManager RM;
    private BlockManagerId blockManagerId;
    private TransactionTopology topology;
    private FileDataGenerator fileDataGenerator = new FileDataGenerator();
    public static MorphStreamEnv ourInstance = new MorphStreamEnv();
    public static MorphStreamEnv get() {
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
    public RdmaShuffleManager RM() {return RM;}
    public BlockManagerId blockManagerId() {return blockManagerId;}
    public FileDataGenerator fileDataGenerator() {return fileDataGenerator;}

    public TransactionTopology topology() {
        return topology;
    }

}
