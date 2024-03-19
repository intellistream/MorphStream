package intellistream.morphstream.api.launcher;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.api.input.FileDataGenerator;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.api.state.DatabaseInitializer;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.impl.RemoteDatabase;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.exception.InvalidIDException;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpout;
import intellistream.morphstream.engine.stream.optimization.OptimizationManager;
import intellistream.morphstream.engine.stream.topology.TopologyBuilder;
import intellistream.morphstream.engine.stream.topology.TopologySubmitter;
import intellistream.morphstream.engine.db.impl.CavaliaDatabase;
import intellistream.morphstream.engine.db.Database;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class MorphStreamEnv {
    private static final Logger LOG = LoggerFactory.getLogger(MorphStreamEnv.class);
    public static MorphStreamEnv ourInstance = new MorphStreamEnv();
    private boolean isDriver;
    private final JCommanderHandler jCommanderHandler = new JCommanderHandler();
    private final Configuration configuration = new Configuration();
    private final FileDataGenerator fileDataGenerator = new FileDataGenerator();
    private final InputSource inputSource = new InputSource();
    private final DatabaseInitializer databaseInitializer = new DatabaseInitializer();
    private Database database;
    private OptimizationManager OM;
    private Topology topology;
    private RdmaWorkerManager workerManager;
    private final ZContext zContext = new ZContext();
    private final TopologyBuilder topologyBuilder = new TopologyBuilder();
    private final TopologySubmitter topologySubmitter = new TopologySubmitter();
    //Clients need to wait for MorphStreamDriver to initialize
    private CountDownLatch clientLatch;//The number of clients + MorphStreamDriver
    //MorphStreamDriver needs to wait for all workers to initialize
    private CountDownLatch workerLatch;//The number of workers
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
    public FileDataGenerator fileDataGenerator() {return fileDataGenerator;}
    public DatabaseInitializer databaseInitializer() {return databaseInitializer;}
    public InputSource inputSource() {return inputSource;}
    public CountDownLatch clientLatch() {return clientLatch;}
    public CountDownLatch workerLatch() {return workerLatch;}
    public RdmaWorkerManager rdmaWorkerManager() {return workerManager;}
    public void setRdmaWorkerManager(RdmaWorkerManager rdmaWorkerManager) {this.workerManager = rdmaWorkerManager;}
    public ZContext zContext() {return zContext;}
    public boolean isDriver() {return isDriver;}
    public void LoadConfiguration(String configPath, String[] args) throws IOException, DatabaseException {
        if (configPath != null) {
            this.jCommanderHandler().loadProperties(configPath);
        }
        JCommander cmd = new JCommander(this.jCommanderHandler());
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            if (enable_log) LOG.error("Argument error: " + ex.getMessage());
            cmd.usage();
            return;
        }
        this.jCommanderHandler().initializeCfg(this.configuration());
        this.isDriver = this.configuration().getBoolean("isDriver", false);
        if (isDriver) {
            this.clientLatch = new CountDownLatch(this.configuration().getInt("clientNum", 1) + 1);// Client Number + MorphStreamDriver
            this.workerLatch = new CountDownLatch(this.configuration().getInt("workerNum", 1));
            InputSourceInitialize();
        } else {
            this.workerLatch = new CountDownLatch(this.configuration().getInt("workerNum", 1));
            DatabaseInitialize();
        }
    }
    public void DatabaseInitialize() throws DatabaseException {
        if (configuration().getBoolean("isRemoteDB", false)) {
            this.database = new RemoteDatabase(configuration);
            //TODO: Implement the remote database
        } else {
            this.database = new CavaliaDatabase(configuration);
            this.databaseInitializer.creates_Table();
            if (configuration.getBoolean("partition", false)) {
                for (int i = 0; i < configuration.getInt("tthread", 4); i++)
                    databaseInitializer.setSpinlock_(i, new SpinLock());
                PartitionedOrderLock.getInstance().initilize(configuration.getInt("tthread", 4));
            }
        }
    }
    public void InputSourceInitialize() throws IOException {
        if (configuration().getInt("inputSourceType", 0) == 0) { //read input as string
            String inputFile = configuration().getString("inputFilePath");
            File file = new File(inputFile);
            if (file.exists()) {
                LOG.info("Data already exists.. skipping data generation...");
                fileDataGenerator().prepareInputData(true);
            } else {
                String fileName = fileDataGenerator().prepareInputData(false);
                configuration().put("inputFilePath", fileName);
            }
            if (fileDataGenerator().getTranToDecisionConf() != null && fileDataGenerator().getTranToDecisionConf().size() != 0){
                StringBuilder stringBuilder = new StringBuilder();
                for(String decision:fileDataGenerator().getTranToDecisionConf()){
                    stringBuilder.append(decision);
                    stringBuilder.append(";");
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                configuration().put("WorkloadConfig", stringBuilder.toString()); //For each workload, how many TD/LD/PD
            }
            inputSource().initialize(configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_STRING, MorphStreamEnv.get().configuration().getInt("clientNum"));
        } else if (configuration().getInt("inputSourceType", 0) == 1) { //read input as JSON
            String inputFile = configuration().getString("inputFilePath");
            File file = new File(inputFile);
            if (file.exists()) {
                LOG.info("Data already exists.. skipping data generation...");
            } else {
                String fileName = fileDataGenerator().prepareInputData(false);
                configuration().put("inputFilePath", fileName);
            }
            inputSource().initialize(configuration().getString("inputFilePath"), InputSource.InputSourceType.FILE_JSON, MorphStreamEnv.get().configuration().getInt("clientNum"));
        }
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
