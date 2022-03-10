package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGenerator;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGeneratorConfig;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.SLTPGDataGenerator;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.SLTPGDataGeneratorConfig;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import db.Database;
import db.DatabaseException;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.SchedulerContext;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.LongDataBox;
import storage.datatype.StringDataBox;
import storage.table.RecordSchema;
import transaction.TableInitilizer;
import utils.AppConfig;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import static common.CONTROL.enable_log;
import static transaction.State.configure_store;

public class SLInitializer extends TableInitilizer {

    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    private final int numberOfStates;
    private final int startingBalance = 1000000;
    private final int partitionOffset;
    private String dataRootPath;
    private DataGenerator dataGenerator;
    private DataGeneratorConfig dataConfig;

    public SLInitializer(Database db, int numberOfStates, double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
        this.numberOfStates = numberOfStates;
        configure_store(theta, tthread, this.numberOfStates);
        this.partitionOffset = this.numberOfStates / tthread;

        this.dataRootPath = config.getString("rootFilePath");
        String generatorType = config.getString("generator");
        switch (generatorType) {
            case "OCGenerator":
                createLayeredOCGenerator(config);
                break;
            case "TPGGenerator":
                createTPGGenerator(config);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + generatorType);
        }
        dataConfig = dataGenerator.getDataConfig();
    }

    protected void createTPGGenerator(Configuration config) {

        SLTPGDataGeneratorConfig dataConfig = new SLTPGDataGeneratorConfig();
        dataConfig.initialize(config);

        configurePath(dataConfig);
        dataGenerator = new SLTPGDataGenerator(dataConfig);
    }

    protected void createLayeredOCGenerator(Configuration config) {

        LayeredOCDataGeneratorConfig dataConfig = new LayeredOCDataGeneratorConfig();
        dataConfig.initialize(config);

        configurePath(dataConfig);
        dataGenerator = new LayeredOCDataGenerator(dataConfig);
    }

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
            if (dataConfig instanceof SLTPGDataGeneratorConfig)
                bytes = digest.digest(String.format("%d_%d_%d_%d_%d_%d_%d_%s",
                                dataConfig.getTotalThreads(),
                                dataConfig.getTotalEvents(),
                                dataConfig.getnKeyStates(),
                                ((SLTPGDataGeneratorConfig) dataConfig).Ratio_Of_Deposit,
                                ((SLTPGDataGeneratorConfig) dataConfig).State_Access_Skewness,
                                ((SLTPGDataGeneratorConfig) dataConfig).Ratio_of_Overlapped_Keys,
                                ((SLTPGDataGeneratorConfig) dataConfig).Ratio_of_Transaction_Aborts,
                                AppConfig.isCyclic)
                        .getBytes(StandardCharsets.UTF_8));
            else
                bytes = digest.digest(String.format("%d_%d_%d",
                                dataConfig.getTotalThreads(),
                                dataConfig.getTotalEvents(),
                                dataConfig.getnKeyStates())
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
            insertAccountRecord(_key, startingBalance, pid, spinlock);
            insertAssetRecord(_key, startingBalance, pid, spinlock);
        }
        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    @Override
    public void loadDB(SchedulerContext context, int thread_id, int NUM_TASK) {
        loadDB(context, thread_id, null, NUM_TASK);
    }

    /**
     * TODO: code clean up to deduplicate.
     *
     * @param context
     * @param thread_id
     * @param spinlock
     * @param NUM_TASK
     */
    @Override
    public void loadDB(SchedulerContext context, int thread_id, SpinLock[] spinlock, int NUM_TASK) {
        int partition_interval = (int) Math.ceil(config.getInt("NUM_ITEMS") / (double) NUM_TASK);
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
            insertAccountRecord(_key, startingBalance, pid, spinlock);
            insertAssetRecord(_key, startingBalance, pid, spinlock);
        }
        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }


    private int[] readRecordMaximumIds() {

        File file = new File(dataRootPath + OsUtils.OS_wrapper("vertices_ids_range.txt"));
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        String[] idsRangeInfo = new String[0];
        try {
            idsRangeInfo = reader.readLine().split(",");
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new int[]{Integer.parseInt(idsRangeInfo[0].split("=")[1]), Integer.parseInt(idsRangeInfo[1].split("=")[1])};
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAccountRecord(String key, long value, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("accounts", new TableRecord(AccountRecord(key, value), pid, spinlock_));
            else
                db.InsertRecord("accounts", new TableRecord(AccountRecord(key, value)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial asset value_list is 0...?
     */
    private void insertAssetRecord(String key, long value, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value), pid, spinlock_));
            else
                db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private SchemaRecord AccountRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        return new SchemaRecord(values);
    }

    private SchemaRecord AssetRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        return new SchemaRecord(values);
    }

    private RecordSchema getRecordSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new LongDataBox());
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema AccountsScheme() {
        return getRecordSchema();
    }

    private RecordSchema BookEntryScheme() {
        return getRecordSchema();
    }

    @Override
    public boolean Generate() {
        String folder = dataConfig.getRootPath();
        File file = new File(folder);
        if (file.exists()) {
            if (enable_log) LOG.info("Data already exists.. skipping data generation...");
            return false;
        }
        file.mkdirs();

        dataGenerator.generateStream();//prepare input events.
        if (enable_log) LOG.info(String.format("Data Generator will dump data at %s.", dataConfig.getRootPath()));
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
            loadTransferDepositEvents(reader, totalEvents, shufflingActive, p_bids);
            reader.close();
        }
    }

    @Override
    public void store(String file_name) throws IOException {

    }

    private void loadTransferDepositEvents(BufferedReader reader, int totalEvents, boolean shufflingActive, int[] p_bids) throws IOException {
        String txn = reader.readLine();
        int count = 0;
//        int[] p_bids = new int[tthread];
        while (txn != null) {
            String[] split = txn.split(",");
            int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
//            int accountTransfer = 100;
//            int bookEntryTransfer = 100;
//            if (count == 10) { // TODO: for the test purpose
//            if (count == 25 || count == 50 || count == 75) {
//                accountTransfer = 100000000;
//                bookEntryTransfer = 100000000;
//            }
            count++;
            if (split.length == 7) {
                HashMap<Integer, Integer> pids = new HashMap<>();
                for (int i = 1; i < 5; i++) {
                    pids.put((int) (Long.parseLong(split[i]) / partitionOffset), 0);
                }
//                List<Integer> list = new ArrayList<>(pids.keySet());
//                Collections.shuffle(list);
                TransactionEvent event = new TransactionEvent(
                        Integer.parseInt(split[0]), //bid
                        npid, //pid
                        Arrays.toString(p_bids), //bid_arrary
                        Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
                        4,//num_of_partition
                        split[1],//getSourceAccountId
                        split[2],//getSourceBookEntryId
                        split[3],//getTargetAccountId
                        split[4],//getTargetBookEntryId
                        Long.parseLong(split[5]), //getAccountTransfer
                        Long.parseLong(split[6])  //getBookEntryTransfer
//                        accountTransfer,
//                        bookEntryTransfer
                );
//                for (int x = 0; x < 4; x++)
//                    p_bids[(npid + x) % tthread]++;
//                pids.replaceAll((k, v) -> p_bids[k]++);
                DataHolder.events.add(event);
            } else if (split.length == 3) {
                HashMap<Integer, Integer> pids = new HashMap<>();
                for (int i = 1; i < 3; i++) {
                    pids.put((int) (Long.parseLong(split[i]) / partitionOffset), 0);
                }
                DepositEvent event = new DepositEvent(
                        Integer.parseInt(split[0]), //bid
                        npid, //pid
                        Arrays.toString(p_bids), //bid_array
                        Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
                        2,//num_of_partition
                        split[1],//getSourceAccountId
                        split[2],//getSourceBookEntryId
                        100,  //getAccountDeposit
                        100  //getBookEntryDeposit
                );
//                for (int x = 0; x < 2; x++)
//                    p_bids[(npid + x) % tthread]++;
//                pids.replaceAll((k, v) -> p_bids[k]++);

                DataHolder.events.add(event);
            }
            if (enable_log) LOG.debug(String.format("%d transactions read...", count));
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


    public void creates_Table(Configuration config) {
        RecordSchema s = AccountsScheme();
        db.createTable(s, "accounts");
        RecordSchema b = BookEntryScheme();
        db.createTable(b, "bookEntries");
        try {
            prepare_input_events(config.getInt("totalEvents"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
