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

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

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
    private final HashMap<Integer, Socket> socketsToInstances = new java.util.HashMap<>();
    private final HashMap<Integer, Integer> stateInstanceMap = new java.util.HashMap<>(); //TODO: Hardcoded
    public String vnfJSON = null;
    private static final HashMap<Integer, Integer> saTypeMap = new HashMap<>(); //State access ID -> state access type
    private static final HashMap<Integer, String> saTableNameMap = new HashMap<>(); //State access ID -> table name
    public static final ConcurrentHashMap<Integer, Object> instanceLocks = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Integer, Integer> fetchedValues = new ConcurrentHashMap<>(); // tupleID -> value

    public void initializeAdaptiveCCManager() {
        if (adaptiveCCManager == null) {
            adaptiveCCManager = new AdaptiveCCManager();
        } else {
            throw new RuntimeException("AdaptiveCCManager already initialized");
        }
    }

    public AdaptiveCCManager getAdaptiveCCManager() {
        return adaptiveCCManager;
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
    public void updateSATypeMap(int saID, int saType) {
        saTypeMap.put(saID, saType);
    }
    public HashMap<Integer, Integer> getSaTypeMap() {return saTypeMap;}
    public void updateSATableNameMap(int saID, String tableName) {
        saTableNameMap.put(saID, tableName);
    }
    public HashMap<Integer, String> getSaTableNameMap() {return saTableNameMap;}

    public void DatabaseInitialize() {
        this.database = new CavaliaDatabase(configuration);
        this.databaseInitializer.creates_Table();
        if (configuration.getBoolean("partition", false)) {
            for (int i = 0; i < configuration.getInt("tthread", 4); i++)
                databaseInitializer.setSpinlock_(i, new SpinLock());
            PartitionedOrderLock.getInstance().initilize(configuration.getInt("tthread", 4));
        }
        for (int i = 0; i < configuration.getInt("tthread", 4); i++) {
            databaseInitializer.loadDB(i, false);
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
