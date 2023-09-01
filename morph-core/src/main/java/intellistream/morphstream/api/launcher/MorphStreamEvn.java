package intellistream.morphstream.api.launcher;

import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.BlockManagerId;
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
    private RdmaShuffleManager RM;
    private BlockManagerId blockManagerId;
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
    public RdmaShuffleManager RM() {return RM;}
    public BlockManagerId blockManagerId() {return blockManagerId;}

    public TransactionTopology topology() {
        return topology;
    }

}
