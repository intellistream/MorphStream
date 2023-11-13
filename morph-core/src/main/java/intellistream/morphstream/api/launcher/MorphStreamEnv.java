package intellistream.morphstream.api.launcher;

import intellistream.morphstream.api.input.FileDataGenerator;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.state.DatabaseInitializer;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.Block.BlockManagerId;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.exception.InvalidIDException;
import intellistream.morphstream.engine.stream.components.grouping.Grouping;
import intellistream.morphstream.engine.stream.components.operators.api.bolt.AbstractBolt;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpout;
import intellistream.morphstream.engine.stream.optimization.OptimizationManager;
import intellistream.morphstream.engine.stream.topology.TopologyBuilder;
import intellistream.morphstream.engine.stream.topology.TopologySubmitter;
import intellistream.morphstream.engine.txn.db.CavaliaDatabase;
import intellistream.morphstream.engine.txn.db.Database;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import org.zeromq.ZContext;

import java.util.concurrent.CountDownLatch;

public class MorphStreamEnv {
    public static MorphStreamEnv ourInstance = new MorphStreamEnv();
    private final JCommanderHandler jCommanderHandler = new JCommanderHandler();
    private final Configuration configuration = new Configuration();
    private final FileDataGenerator fileDataGenerator = new FileDataGenerator();
    private final InputSource inputSource = new InputSource();
    private final DatabaseInitializer databaseInitializer = new DatabaseInitializer();
    private Database database;
    private OptimizationManager OM;
    private RdmaShuffleManager RM;
    private BlockManagerId blockManagerId;
    private Topology topology;
    private final TopologyBuilder topologyBuilder = new TopologyBuilder();
    private final TopologySubmitter topologySubmitter = new TopologySubmitter();
    private final ZContext zContext = new ZContext();
    private CountDownLatch latch;//The number of clients + MorphStreamWorker
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
    public DatabaseInitializer databaseInitializer() {return databaseInitializer;}
    public ZContext zContext() {return zContext;}
    public InputSource inputSource() {return inputSource;}
    public CountDownLatch latch() {return latch;}
    public void DatabaseInitialize() {
        this.database = new CavaliaDatabase(configuration);
        this.databaseInitializer.creates_Table();
        if (configuration.getBoolean("partition", false)) {
            for (int i = 0; i < configuration.getInt("tthread", 4); i++)
                databaseInitializer.setSpinlock_(i, new SpinLock());
            PartitionedOrderLock.getInstance().initilize(configuration.getInt("tthread", 4));
        }
    }
    public void CountDownLatchInitialize(int num) {
        this.latch = new CountDownLatch(num);
    }
    public void setSpout(String id, AbstractSpout spout, int numTasks) {
        try {
            topologyBuilder.setSpout(id, spout, numTasks);
        } catch (InvalidIDException e) {
            throw new RuntimeException(e);
        }
    }
    public Topology createTopology() {
        return topologyBuilder.createTopology();
    }
    public void submitTopology(Topology topology) {
        try {
            this.topology = topologySubmitter.submitTopology(topology, configuration);
            this.OM = topologySubmitter.getOM();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Topology getTopology() {
        return topology;
    }
}
