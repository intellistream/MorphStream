package intellistream.morphstream.examples.tsp.onlinebiding.util;

import intellistream.morphstream.engine.txn.DataHolder;
import intellistream.morphstream.examples.utils.datagen.DataGenerator;
import intellistream.morphstream.examples.utils.datagen.DataGeneratorConfig;
import intellistream.morphstream.examples.tsp.onlinebiding.events.OBTPGDynamicDataGenerator;
import intellistream.morphstream.examples.utils.datagen.DynamicDataGeneratorConfig;
import intellistream.morphstream.examples.tsp.onlinebiding.events.AlertTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.BuyingTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.ToppingTxnEvent;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.db.Database;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.datatype.IntDataBox;
import intellistream.morphstream.engine.txn.storage.datatype.LongDataBox;
import intellistream.morphstream.engine.txn.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.transaction.TableInitilizer;
import intellistream.morphstream.util.AppConfig;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Constant.MAX_Price;
import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Constant.NUM_ACCESSES_PER_BUY;
import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.CONTROL.enable_states_partition;
import static intellistream.morphstream.configuration.Constants.Event_Path;
import static intellistream.morphstream.engine.txn.profiler.Metrics.NUM_ITEMS;
import static intellistream.morphstream.engine.txn.transaction.State.configure_store;

//import static xerial.jnuma.Numa.setLocalAlloc;
public class OBInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(OBInitializer.class);
    private final int i = 0;
    private final int numberOfStates;
    private final DataGeneratorConfig dataConfig;
    private final int partitionOffset;
    private final int NUM_ACCESS;
    private final int Transaction_Length;
    private final SplittableRandom rnd = new SplittableRandom();
    //triple decisions
    protected int[] triple_decision = new int[]{0, 0, 0, 0, 0, 0, 1, 2};//6:1:1 buy, alert, topping_handle.
    private String dataRootPath;
    private DataGenerator dataGenerator;

    public OBInitializer(Database db, int numberOfStates, double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
        floor_interval = (int) Math.floor(numberOfStates / (double) tthread);//NUM_ITEMS / tthread;
        this.dataRootPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("inputs");
        this.partitionOffset = numberOfStates / tthread;
        this.NUM_ACCESS = config.getInt("NUM_ACCESS");
        this.Transaction_Length = config.getInt("Transaction_Length");
        this.numberOfStates = numberOfStates;
        configure_store(theta, tthread, NUM_ITEMS);
        createTPGGenerator(config);
        dataConfig = dataGenerator.getDataConfig();
    }

    protected void createTPGGenerator(Configuration config) {
        DynamicDataGeneratorConfig dynamicDataGeneratorConfig = new DynamicDataGeneratorConfig();
        dynamicDataGeneratorConfig.initialize(config);
        configurePath(dynamicDataGeneratorConfig);
        dataGenerator = new OBTPGDynamicDataGenerator(dynamicDataGeneratorConfig);
    }

    private void configurePath(DynamicDataGeneratorConfig dynamicDataGeneratorConfig) {
        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes;
            bytes = digest.digest(String.format("%d_%d_%d_%s_%s_%s",
                            dynamicDataGeneratorConfig.getTotalThreads(),
                            dynamicDataGeneratorConfig.getTotalEvents(),
                            dynamicDataGeneratorConfig.getnKeyStates(),
                            dynamicDataGeneratorConfig.getApp(),
                            AppConfig.isCyclic)
                    .getBytes(StandardCharsets.UTF_8));
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(bytes));
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        dynamicDataGeneratorConfig.setRootPath(dynamicDataGeneratorConfig.getRootPath() + OsUtils.OS_wrapper(subFolder));
        dynamicDataGeneratorConfig.setIdsPath(dynamicDataGeneratorConfig.getIdsPath() + OsUtils.OS_wrapper(subFolder));
        this.dataRootPath += OsUtils.OS_wrapper(subFolder);
    }

    @Override
    public void loadDB(int thread_id, int NUMTasks) {
        int partition_interval = (int) Math.ceil(numberOfStates / (double) NUMTasks);
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUMTasks - 1) {//last executor need to handle left-over
            right_bound = numberOfStates;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            insertItemRecords(key, 100, thread_id);
        }
        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUMTasks) {
        int partition_interval = (int) Math.ceil(numberOfStates / (double) NUMTasks);
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUMTasks - 1) {//last executor need to handle left-over
            right_bound = numberOfStates;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);
            insertItemRecords(key, 100, pid, spinlock, thread_id);
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

    /**
     * 4 + 8 + 8
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertItemRecords(int key, long value, int partition_id) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord, this.tthread), partition_id);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertItemRecords(int key, long value, int pid, SpinLock[] spinlock_, int partition_id) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord, pid, spinlock_), partition_id);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private RecordSchema Goods() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new LongDataBox());
        fieldNames.add("ID");//PK
        fieldNames.add("Price");
        fieldNames.add("Qty");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    @Override
    public boolean Generate() throws IOException {
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
        long[] p_bids = new long[tthread];
        if (file.exists()) {
            if (enable_log) LOG.info("Reading transfer events...");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            loadOnlineBiddingEvents(reader, totalEvents, shufflingActive, p_bids);
            reader.close();
        }
    }

    private void loadOnlineBiddingEvents(BufferedReader reader, int totalEvents, boolean shufflingActive, long[] p_bids) throws IOException {
        String txn = reader.readLine();
        int count = 0;
        while (txn != null) {
            TxnEvent event;
            String[] split = txn.split(",");
            if (split.length > 3) {
                int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
                int keyLength = Transaction_Length;
                HashMap<Integer, Integer> pids = new HashMap<>();
                int[] keys = new int[keyLength];
                for (int i = 1; i < keyLength + 1; i++) {
                    keys[i - 1] = Integer.parseInt(split[i]);
                    pids.put(keys[i - 1] / partitionOffset, 0);
                }
                if (split[split.length - 1].equals("1")) {
                    event = new AlertTxnEvent(keyLength, keys, rnd, npid, Arrays.toString(p_bids), Integer.parseInt(split[0]), pids.size(), Arrays.toString(pids.keySet().toArray(new Integer[0])));
                } else {
                    event = new ToppingTxnEvent(keyLength, keys, rnd, npid, Arrays.toString(p_bids), Integer.parseInt(split[0]), pids.size(), Arrays.toString(pids.keySet().toArray(new Integer[0])));
                }
            } else {
                int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
                int[] keys = new int[NUM_ACCESSES_PER_BUY];
                HashMap<Integer, Integer> pids = new HashMap<>();
                for (int i = 1; i < NUM_ACCESSES_PER_BUY + 1; i++) {
                    keys[i - 1] = Integer.parseInt(split[i]);
                    pids.put(keys[i - 1] / partitionOffset, 0);
                }
                if (split[split.length - 1].equals("true")) {
                    event = new BuyingTxnEvent(keys, npid, Arrays.toString(p_bids), Integer.parseInt(split[0]), pids.size(), Arrays.toString(pids.keySet().toArray(new Integer[0])), true);
                } else {
                    event = new BuyingTxnEvent(keys, npid, Arrays.toString(p_bids), Integer.parseInt(split[0]), pids.size(), Arrays.toString(pids.keySet().toArray(new Integer[0])), false);
                }
            }
            DataHolder.txnEvents.add(event);
            if (enable_log) LOG.debug(String.format("%d deposit read...", count));
            txn = reader.readLine();
        }
        if (enable_log) LOG.info("Done reading transfer events...");
        if (shufflingActive) {
            shuffleEvents(DataHolder.txnEvents, totalEvents);
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
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition);
        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist
        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(event_path + OsUtils.OS_wrapper(file_name))));
        for (Object event : db.getEventManager().input_events) {
            StringBuilder sb = new StringBuilder();
            if (event instanceof BuyingTxnEvent) {
                sb.append(((BuyingTxnEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((BuyingTxnEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingTxnEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((BuyingTxnEvent) event).num_p());//3
                sb.append(split_exp);
                sb.append("BuyingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingTxnEvent) event).getItemId()));//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingTxnEvent) event).getBidPrice()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingTxnEvent) event).getBidQty()));//7
            } else if (event instanceof AlertTxnEvent) {
                sb.append(((AlertTxnEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((AlertTxnEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertTxnEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((AlertTxnEvent) event).num_p());
                sb.append(split_exp);
                sb.append("AlertEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((AlertTxnEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertTxnEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertTxnEvent) event).getAsk_price()));
            } else if (event instanceof ToppingTxnEvent) {
                sb.append(((ToppingTxnEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((ToppingTxnEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingTxnEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((ToppingTxnEvent) event).num_p());
                sb.append(split_exp);
                sb.append("ToppingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((ToppingTxnEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingTxnEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingTxnEvent) event).getItemTopUp()));
            }
            w.write(sb + "\n");
        }
        w.close();
    }

    @Override
    public List<String> getTranToDecisionConf() {
        return dataGenerator.getTranToDecisionConf();
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = Goods();
        db.createTable(s, "goods", config.getInt("tthread"), config.getInt("NUM_ITEMS"));
        try {
            prepare_input_events(config.getInt("totalEvents"));
            if (getTranToDecisionConf() != null && getTranToDecisionConf().size() != 0) {//input data already exist
                StringBuilder stringBuilder = new StringBuilder();
                for (String decision : getTranToDecisionConf()) {
                    stringBuilder.append(decision);
                    stringBuilder.append(";");
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                config.put("WorkloadConfig", stringBuilder.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}