package intellistream.morphstream.api.launcher;

import intellistream.morphstream.api.input.AdaptiveCCManager;
import intellistream.morphstream.api.state.DatabaseInitializer;
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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class MorphStreamEnv {
    public static MorphStreamEnv ourInstance = new MorphStreamEnv();
    private final JCommanderHandler jCommanderHandler = new JCommanderHandler();
    private final Configuration configuration = new Configuration();
    private AdaptiveCCManager adaptiveCCManager;
    private final DatabaseInitializer databaseInitializer = new DatabaseInitializer();
    private Database database;
    private OptimizationManager OM;
    private Topology topology;
    private final TopologyBuilder topologyBuilder = new TopologyBuilder();
    private final TopologySubmitter topologySubmitter = new TopologySubmitter();
    private ServerSocket stateManagerSocket;
    private static int stateManagerPort = 12000;
    private final HashMap<Integer, Socket> socketsToInstances = new java.util.HashMap<>();
    private final HashMap<Integer, Integer> stateInstanceMap = new java.util.HashMap<>(); //TODO: Hardcoded

    public MorphStreamEnv() {
        try {
            stateManagerSocket = new ServerSocket(stateManagerPort);
            System.out.println("Server started on port " + stateManagerPort);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void initializeAdaptiveCCManager() throws IOException {
        if (adaptiveCCManager == null) {
            adaptiveCCManager = new AdaptiveCCManager();
            adaptiveCCManager.initialize();
        }
    }

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
    public DatabaseInitializer databaseInitializer() {return databaseInitializer;}
    public void addInstanceSocket(Integer instanceID, Socket socket) {
        socketsToInstances.put(instanceID, socket);
    }
    public Map<Integer, Socket> instanceSocketMap() {return socketsToInstances;}
    public HashMap<Integer, Integer> stateInstanceMap() {return stateInstanceMap;}
    public ServerSocket stateManagerSocket() {return stateManagerSocket;}
    public AdaptiveCCManager adaptiveCCManager() {
        if (adaptiveCCManager == null) {
            try {
                initializeAdaptiveCCManager();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return adaptiveCCManager;
    }
    public void DatabaseInitialize() {
        this.database = new CavaliaDatabase(configuration);
        this.databaseInitializer.creates_Table();
        if (configuration.getBoolean("partition", false)) {
            for (int i = 0; i < configuration.getInt("tthread", 4); i++)
                databaseInitializer.setSpinlock_(i, new SpinLock());
            PartitionedOrderLock.getInstance().initilize(configuration.getInt("tthread", 4));
        }
    }
    public void setSpout(String id, AbstractSpout spout, int numTasks) {
        try {
            topologyBuilder.setSpout(id, spout, numTasks);
        } catch (InvalidIDException e) {
            throw new RuntimeException(e);
        }
    }
    public void setBolt(String id, AbstractBolt b, int numTasks, Grouping... groups) {
        try {
            topologyBuilder.setBolt(id, b, numTasks, groups);
        } catch (InvalidIDException e) {
            throw new RuntimeException(e);
        }
    }
    public void setSink(String id, AbstractBolt b, int numTasks, Grouping... groups) {
        try {
            topologyBuilder.setSink(id, b, numTasks, groups);
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
