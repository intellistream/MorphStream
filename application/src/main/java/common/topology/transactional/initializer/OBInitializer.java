package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.apps.OB.OBTPGDynamicDataGenerator;
import benchmark.dynamicWorkloadGenerator.DynamicDataGeneratorConfig;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.ob.AlertEvent;
import common.param.ob.BuyingEvent;
import common.param.ob.ToppingEvent;
import db.Database;
import db.DatabaseException;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.SchedulerContext;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.IntDataBox;
import storage.datatype.LongDataBox;
import storage.table.RecordSchema;
import transaction.TableInitilizer;
import utils.AppConfig;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;
import static common.Constants.Event_Path;
import static common.constants.OnlineBidingSystemConstants.Constant.*;
import static profiler.Metrics.NUM_ITEMS;
import static transaction.State.configure_store;

//import static xerial.jnuma.Numa.setLocalAlloc;
public class OBInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(OBInitializer.class);
    //triple decisions
    protected int[] triple_decision = new int[]{0, 0, 0, 0, 0, 0, 1, 2};//6:1:1 buy, alert, topping_handle.
    private int i = 0;
    private final int numberOfStates;
    private String dataRootPath;
    private DataGenerator dataGenerator;
    private final DataGeneratorConfig dataConfig;
    private final int partitionOffset;
    private final int NUM_ACCESS;
    private final int Transaction_Length;
    private SplittableRandom rnd = new SplittableRandom();

    public OBInitializer(Database db, int numberOfStates,double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
        floor_interval = (int) Math.floor(numberOfStates / (double) tthread);//NUM_ITEMS / tthread;
        this.dataRootPath = config.getString("rootFilePath") + OsUtils.osWrapperPostFix("stats");
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
        int partition_interval = (int) Math.ceil(numberOfStates / (double) NUMTasks);;
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUMTasks - 1) {//last executor need to handle left-over
            right_bound = numberOfStates;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            insertItemRecords(key, 100);
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
            insertItemRecords(key, 100, pid, spinlock);
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
    private void insertItemRecords(int key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertItemRecords(int key, long value, int pid, SpinLock[] spinlock_) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord, pid, spinlock_));
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
                int keyLength = 1*Transaction_Length;
                HashMap<Integer, Integer> pids = new HashMap<>();
                int[] keys = new int[keyLength];
                for (int i = 1; i < keyLength+1; i++) {
                    keys[i-1] = Integer.parseInt(split[i]);
                    pids.put((int) (keys[i-1] / partitionOffset), 0);
                }
                if (split[split.length-1].equals("1")) {
                    event = new AlertEvent(keyLength, keys, rnd, npid,  Arrays.toString(p_bids), Integer.parseInt(split[0]), pids.size(),Arrays.toString(pids.keySet().toArray(new Integer[0])));
                } else {
                    event = new ToppingEvent(keyLength, keys, rnd, npid, Arrays.toString(p_bids), Integer.parseInt(split[0]), pids.size(),Arrays.toString(pids.keySet().toArray(new Integer[0])));
                }
            } else {
                int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
                int[] keys = new int[NUM_ACCESSES_PER_BUY];
                HashMap<Integer, Integer> pids = new HashMap<>();
                for (int i = 1; i < NUM_ACCESSES_PER_BUY+1; i++) {
                    keys[i-1] = Integer.parseInt(split[i]);
                    pids.put((int) (keys[i-1] / partitionOffset), 0);
                }
                if (split[split.length-1].equals("true")) {
                    event = new BuyingEvent(keys,npid, Arrays.toString(p_bids),Integer.parseInt(split[0]),pids.size(),Arrays.toString(pids.keySet().toArray(new Integer[0])),true);
                } else {
                    event = new BuyingEvent(keys,npid, Arrays.toString(p_bids),Integer.parseInt(split[0]),pids.size(),Arrays.toString(pids.keySet().toArray(new Integer[0])),false);
                }
            }
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
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition);
        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist
        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(event_path + OsUtils.OS_wrapper(file_name))));
        for (Object event : db.getEventManager().input_events) {
            StringBuilder sb = new StringBuilder();
            if (event instanceof BuyingEvent) {
                sb.append(((BuyingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).num_p());//3
                sb.append(split_exp);
                sb.append("BuyingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getItemId()));//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidPrice()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidQty()));//7
            } else if (event instanceof AlertEvent) {
                sb.append(((AlertEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((AlertEvent) event).num_p());
                sb.append(split_exp);
                sb.append("AlertEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getAsk_price()));
            } else if (event instanceof ToppingEvent) {
                sb.append(((ToppingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).num_p());
                sb.append(split_exp);
                sb.append("ToppingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemTopUp()));
            }
            w.write(sb.toString() + "\n");
        }
        w.close();
    }

    @Override
    public List<String> getTranToDecisionConf() {
        return dataGenerator.getTranToDecisionConf();
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = Goods();
        db.createTable(s, "goods");
        try {
            prepare_input_events(config.getInt("totalEvents"));
            if (getTranToDecisionConf() != null && getTranToDecisionConf().size() != 0){//input data already exist
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
