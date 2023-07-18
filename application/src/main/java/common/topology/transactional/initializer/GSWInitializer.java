package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.apps.GSW.TPGTxnGenerator.GSWTPGDataGenerator;
import benchmark.datagenerator.apps.GSW.TPGTxnGenerator.GSWTPGDataGeneratorConfig;
import benchmark.datagenerator.apps.GSW.TPGTxnGenerator.GSWTPGDynamicDataGenerator;
import benchmark.dynamicWorkloadGenerator.DynamicDataGeneratorConfig;
import common.collections.Configuration;
import common.collections.OsUtils;
import engine.txn.TxnEvent;
import common.param.gsw.WindowedMicroEvent;
import common.param.mb.MicroEvent;
import engine.txn.db.Database;
import engine.txn.db.DatabaseException;
import engine.txn.lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.scheduler.context.SchedulerContext;
import engine.txn.storage.SchemaRecord;
import engine.txn.storage.TableRecord;
import engine.txn.storage.datatype.DataBox;
import engine.txn.storage.datatype.IntDataBox;
import engine.txn.storage.datatype.LongDataBox;
import engine.txn.storage.datatype.StringDataBox;
import engine.txn.storage.table.RecordSchema;
import engine.txn.transaction.TableInitilizer;
import util.AppConfig;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;
import static common.Constants.Event_Path;
import static engine.txn.profiler.Metrics.NUM_ITEMS;
import static engine.txn.transaction.State.configure_store;

public class GSWInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(GSWInitializer.class);
    private final int numberOfStates;
    private final int startingValue = 10000;
    //different R-W ratio.
    //just enable one of the decision array
    protected transient boolean[] read_decision;
    int i = 0;
    private String dataRootPath;
    private DataGenerator dataGenerator;
    private final DataGeneratorConfig dataConfig;
    private final int partitionOffset;
    private final int NUM_ACCESS;
    private final int Transaction_Length;


    public GSWInitializer(Database db, int numberOfStates, double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
        floor_interval = (int) Math.floor(numberOfStates / (double) tthread);//NUM_ITEMS / tthread;
        this.dataRootPath = config.getString("rootFilePath");
        this.partitionOffset = numberOfStates / tthread;
        this.NUM_ACCESS = config.getInt("NUM_ACCESS");
        this.Transaction_Length = config.getInt("Transaction_Length");
        this.numberOfStates = numberOfStates;
        // set up generator
        configure_store(theta, tthread, numberOfStates);
        createTPGGenerator(config);
        dataConfig = dataGenerator.getDataConfig();
    }

    protected void createTPGGenerator(Configuration config) {
        if(config.getBoolean("isDynamic")) {
            //TODO:add the dynamic workload dataGenerator
            DynamicDataGeneratorConfig dynamicDataGeneratorConfig=new DynamicDataGeneratorConfig();
            dynamicDataGeneratorConfig.initialize(config);
            configurePath(dynamicDataGeneratorConfig);
            dataGenerator = new GSWTPGDynamicDataGenerator(dynamicDataGeneratorConfig);
        }else {
            GSWTPGDataGeneratorConfig dataConfig = new GSWTPGDataGeneratorConfig();
            dataConfig.initialize(config);

            configurePath(dataConfig);
            dataGenerator = new GSWTPGDataGenerator(dataConfig);
        }
    }
    /**
     * Control the input file path.
     * TODO: think carefully which configuration shall vary.
     *
     * @param config
     */
    /**
     * Control the input file path.
     * TODO: think carefully which configuration shall vary.
     *
     * @param dataConfig
     */
    private void configurePath(DataGeneratorConfig dataConfig) {
        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes;
            if (dataConfig instanceof GSWTPGDataGeneratorConfig)
            bytes = digest.digest(String.format("%d_%d_%d_%d_%d_%d_%d_%d_%d_%s",
                            dataConfig.getTotalThreads(),
                            dataConfig.getTotalEvents(),
                            dataConfig.getnKeyStates(),
                            ((GSWTPGDataGeneratorConfig) dataConfig).NUM_ACCESS,
                            ((GSWTPGDataGeneratorConfig) dataConfig).State_Access_Skewness,
                            ((GSWTPGDataGeneratorConfig) dataConfig).Ratio_of_Overlapped_Keys,
                            ((GSWTPGDataGeneratorConfig) dataConfig).Period_of_Window_Reads,
                            ((GSWTPGDataGeneratorConfig) dataConfig).Transaction_Length,
                            ((GSWTPGDataGeneratorConfig) dataConfig).Ratio_of_Multiple_State_Access,
                            AppConfig.isCyclic)
                        .getBytes(StandardCharsets.UTF_8));
            else
                bytes = digest.digest(String.format("%d_%d_%d_%s_%s_%s",
                                dataConfig.getTotalThreads(),
                                dataConfig.getTotalEvents(),
                                dataConfig.getnKeyStates(),
                                ((DynamicDataGeneratorConfig) dataConfig).getApp(),
                                AppConfig.isCyclic)
                        .getBytes(StandardCharsets.UTF_8));
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(bytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataConfig.setRootPath(dataConfig.getRootPath() + OsUtils.OS_wrapper(subFolder));
        dataConfig.setIdsPath(dataConfig.getIdsPath() + OsUtils.OS_wrapper(subFolder));
        this.dataRootPath += OsUtils.OS_wrapper(subFolder);
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertMicroRecord(String key, long value, int pid, SpinLock[] spinlock_, int partition_id) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("MicroTable", new TableRecord(Record(key, value), pid, spinlock_), partition_id);
            else
                db.InsertRecord("MicroTable", new TableRecord(Record(key, value), this.tthread), partition_id);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private SchemaRecord Record(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        return new SchemaRecord(values);
    }

    @Override
    public void loadDB(int thread_id, int NUM_TASK) {
        loadDB(thread_id, null, NUM_TASK);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUM_TASK) {
        int partition_interval = (int) Math.ceil(numberOfStates / (double) NUM_TASK);
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUM_TASK - 1) {//last executor need to handle left-over
            right_bound = config.getInt("NUM_ITEMS");
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        int pid;
        String _key;
        for (int key = left_bound; key < right_bound; key++) {
            pid = get_pid(partition_interval, key);
            _key = String.valueOf(key);
//            assert value.length() == VALUE_LEN;
            insertMicroRecord(_key, startingValue , pid, spinlock, thread_id);
        }
        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }


    @Override
    public void loadDB(SchedulerContext context, int thread_id, int NUMTasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadDB(SchedulerContext context, int thread_id, SpinLock[] spinlock, int NUMTasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean Generate() {
        String folder = dataRootPath;
        File file = new File(folder);
        if (file.exists()) {
            if (enable_log) LOG.info("Data already exists.. skipping data generation...");
            return false;
        }
        file.mkdirs();

        dataGenerator.generateStream();//prepare input events.
        if (enable_log) LOG.info(String.format("Data Generator will dump data at %s.", dataRootPath));
        dataGenerator.dumpGeneratedDataToFile();
        if (enable_log) LOG.info("Data Generation is done...");
        dataGenerator.clearDataStructures();
        return true;
    }

    @Override
    protected void Load() throws IOException {
        int totalEvents = dataConfig.getTotalEvents();
        boolean shufflingActive = dataConfig.getShufflingActive();
        String folder = dataConfig.getRootPath();
        File file = new File(folder + "events.txt");
        int[] p_bids = new int[tthread];
        if (file.exists()) {
            if (enable_log) LOG.info("Reading transfer events...");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            loadWindowedMicroEvents(reader, totalEvents, shufflingActive, p_bids);
            reader.close();
        }
    }

    private void loadWindowedMicroEvents(BufferedReader reader, int totalEvents, boolean shufflingActive, int[] p_bids) throws IOException {
        String txn = reader.readLine();
        int count = 0;
//        int p_bids[] = new int[tthread];
        while (txn != null) {
            String[] split = txn.split(",");
            int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
            // construct bid array
            int keyLength = split.length - 2;
            HashMap<Integer, Integer> pids = new HashMap<>();
            long[] keys = new long[keyLength];
            for (int i = 1; i < keyLength+1; i++) {
                keys[i-1] = Long.parseLong(split[i]);
                pids.put((int) (keys[i-1] / partitionOffset), 0);
            }

            // construct event
            WindowedMicroEvent event = new WindowedMicroEvent(
                    Integer.parseInt(split[0]), //bid,
                    npid, //pid
                    Arrays.toString(p_bids), //bid_array
                    Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
                    pids.size(), // num_of_partition
                    Arrays.toString(keys), // key_array
                    keyLength,
                    Transaction_Length,
                    Boolean.parseBoolean(split[keyLength+1]));
            DataHolder.events.add(event);
            if (enable_log) LOG.debug(String.format("%d deposit read...", count));
            txn = reader.readLine();
        }
        if (enable_log) LOG.info("Done reading transfer events...");
        if (shufflingActive) {
            shuffleEvents(DataHolder.events, totalEvents);
        }
    }

    private void shuffleEvents(ArrayList<TxnEvent> txnEvents, int totalEvents) {
        Random random = new Random();
        int index;
        TxnEvent temp;
        for (int i = totalEvents - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = txnEvents.get(index);
            txnEvents.set(index, txnEvents.get(i));
            txnEvents.set(i, temp);
        }
    }

    @Override
    public void store(String file_name) throws IOException {
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
        this.number_partitions = Math.min(tthread, config.getInt("number_partitions"));
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition)
                + OsUtils.OS_wrapper("NUM_EVENTS=" + config.getInt("totalEvents"))
                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + ratio_of_multi_partition)
                + OsUtils.OS_wrapper("number_partitions=" + number_partitions)
                + OsUtils.OS_wrapper("ratio_of_read=" + ratio_of_read)
                + OsUtils.OS_wrapper("NUM_ACCESS=" + NUM_ACCESS)
                + OsUtils.OS_wrapper("theta=" + theta)
                + OsUtils.OS_wrapper("NUM_ITEMS=" + NUM_ITEMS);
        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist
        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(event_path + OsUtils.OS_wrapper(file_name))));
        for (Object event : db.getEventManager().input_events) {
            MicroEvent microEvent = (MicroEvent) event;
            String sb =
                    microEvent.getBid() +//0 -- bid
                            split_exp +
                            microEvent.getPid() +//1
                            split_exp +
                            Arrays.toString(microEvent.getBid_array()) +//2
                            split_exp +
                            microEvent.num_p() +//3 num of p
                            split_exp +
                            "MicroEvent" +//4 input_event types.
                            split_exp +
                            Arrays.toString(microEvent.getKeys()) +//5 keys
                            split_exp +
                            microEvent.ABORT_EVENT()//6
                    ;
            w.write(sb
                    + "\n");
        }
        w.close();
    }

    @Override
    public List<String> getTranToDecisionConf() {
        return dataGenerator.getTranToDecisionConf();
    }

    private RecordSchema MicroTableSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new StringDataBox());
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = MicroTableSchema();
        db.createTable(s, "MicroTable",config.getInt("tthread"), config.getInt("NUM_ITEMS"));
        try {
            prepare_input_events(config.getInt("totalEvents"));
            if (getTranToDecisionConf() != null && getTranToDecisionConf().size() !=0){
                StringBuilder stringBuilder = new StringBuilder();
                for(String decision:getTranToDecisionConf()){
                    stringBuilder.append(decision);
                    stringBuilder.append(";");
                }
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
                config.put("WorkloadConfig",stringBuilder.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
