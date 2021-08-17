package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGenerator;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGeneratorConfig;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.TPGDataGenerator;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.TPGDataGeneratorConfig;
import common.SpinLock;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import db.Database;
import db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.LongDataBox;
import storage.datatype.StringDataBox;
import storage.table.RecordSchema;
import transaction.TableInitilizer;
import transaction.scheduler.SchedulerContext;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static transaction.State.configure_store;

public class SLInitializer extends TableInitilizer {

    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    private final int totalRecords;
    private final String idsGenType;
    private final int numberOfStates;
    private final int startingBalance = 1000000;
    private final int partitionOffset;
    private String dataRootPath;
    private DataGenerator dataGenerator;
    private DataGeneratorConfig dataConfig;

    public SLInitializer(Database db, String dataRootPath, int numberOfStates, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        this.numberOfStates = numberOfStates;
        this.dataRootPath = dataRootPath;
        configure_store(scale_factor, theta, tthread, this.numberOfStates);
        totalRecords = config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches");
        idsGenType = config.getString("idGenType");
        this.partitionOffset = this.numberOfStates / tthread;

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

    protected void createLayeredOCGenerator(Configuration config) {

        LayeredOCDataGeneratorConfig dataConfig = new LayeredOCDataGeneratorConfig();
        dataConfig.initialize(config);

        configurePath(dataConfig);
        dataGenerator = new LayeredOCDataGenerator(dataConfig);
    }

    protected void createTPGGenerator(Configuration config) {

        TPGDataGeneratorConfig dataConfig = new TPGDataGeneratorConfig();
        dataConfig.initialize(config);

        configurePath(dataConfig);
        dataGenerator = new TPGDataGenerator(dataConfig);
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
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(
                            digest.digest(
                                    String.format("%d_%d_%d",
                                                    dataConfig.getTotalThreads(),
                                                    dataConfig.getTuplesPerBatch(),
                                                    dataConfig.getTotalBatches()
                                            )
                                            .getBytes(StandardCharsets.UTF_8))));
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
        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);
            String _key = String.valueOf(key);
            insertAccountRecord(_key, startingBalance, pid, spinlock);
            insertAssetRecord(_key, startingBalance, pid, spinlock);
        }
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
        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);
            String _key = String.valueOf(key);
            insertAccountRecord(_key, startingBalance, pid, spinlock);
            context.UpdateMapping("accounts" + "|" + _key);
            insertAssetRecord(_key, startingBalance, pid, spinlock);
            context.UpdateMapping("bookEntries" + "|" + _key);
        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
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
            LOG.info("Data already exists.. skipping data generation...");
            return false;
        }
        file.mkdirs();

        dataGenerator.generateStream();//prepare input events.
        LOG.info(String.format("Data Generator will dump data at %s.", dataConfig.getRootPath()));
        dataGenerator.dumpGeneratedDataToFile();
        LOG.info("Data Generation is done...");
        dataGenerator.clearDataStructures();
        return true;
    }

    @Override
    protected void Load() throws IOException {
        int tuplesPerBatch = dataConfig.getTuplesPerBatch();
        int totalBatches = dataConfig.getTotalBatches();
        boolean shufflingActive = dataConfig.getShufflingActive();
        String folder = dataConfig.getRootPath();
        File file = new File(folder + "transferEvents.txt");
        if (file.exists()) {
            LOG.info("Reading transfer events...");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            loadTransferEvents(reader, tuplesPerBatch, totalBatches, shufflingActive);
            reader.close();
        }

        file = new File(folder + "depositEvents.txt");
        if (file.exists()) {
            LOG.info("Reading deposit events...");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            loadDepositEvents(reader, tuplesPerBatch, totalBatches, shufflingActive);
            reader.close();
        }
    }

    @Override
    public void store(String file_name) throws IOException {

    }

    private void loadDepositEvents(BufferedReader reader, int tuplesPerBatch, int totalBatches, boolean shufflingActive) throws IOException {
        String txn = reader.readLine();
        int count = 0;
        int p_bids[] = new int[tthread];
        while (txn != null) {
            String[] split = txn.split(",");
            int npid = (int) (Long.valueOf(split[1]) / partitionOffset);
            DepositEvent event = new DepositEvent(
                    Integer.parseInt(split[0]), //bid
                    npid, //pid
                    Arrays.toString(p_bids), //bid_array
                    2,//num_of_partition
                    split[1],//getSourceAccountId
                    split[2],//getSourceBookEntryId
                    100,  //getAccountDeposit
                    100  //getBookEntryDeposit
            );
            for (int x = 0; x < 4; x++)
                p_bids[(npid + x) % tthread]++;
            DataHolder.depositEvents.add(event);
            LOG.debug(String.format("%d deposit read...", count));
            txn = reader.readLine();
        }
        LOG.info("Done reading transfer events...");
        if (shufflingActive) {
            shuffleEvents(DataHolder.depositEvents, tuplesPerBatch, totalBatches);
        }
    }

    private void loadTransferEvents(BufferedReader reader, int tuplesPerBatch, int totalBatches, boolean shufflingActive) throws IOException {
        String txn = reader.readLine();
        int count = 0;
        int p_bids[] = new int[tthread];
        while (txn != null) {
            String[] split = txn.split(",");
            int npid = (int) (Long.valueOf(split[1]) / partitionOffset);
            int accountTransfer = 100;
            int accountEntryTransfer = 100;
            if (count == 500 || count == 5000 || count == 50000) {
                accountTransfer = 100000000;
                accountEntryTransfer = 100000000;
            }

            count++;
            TransactionEvent event = new TransactionEvent(
                    Integer.parseInt(split[0]), //bid
                    npid, //pid
                    Arrays.toString(p_bids), //bid_arrary
                    4,//num_of_partition
                    split[1],//getSourceAccountId
                    split[2],//getSourceBookEntryId
                    split[3],//getTargetAccountId
                    split[4],//getTargetBookEntryId
                    accountTransfer,  //getAccountTransfer
                    accountEntryTransfer  //getBookEntryTransfer
            );
            for (int x = 0; x < 4; x++)
                p_bids[(npid + x) % tthread]++;
            DataHolder.transferEvents.add(event);
            LOG.debug(String.format("%d transactions read...", count));
            txn = reader.readLine();
        }
        LOG.info("Done reading transfer events...");
        if (shufflingActive) {
            shuffleEvents(DataHolder.transferEvents, tuplesPerBatch, totalBatches);
        }
    }

    private void shuffleEvents(ArrayList<TxnEvent> txnEvents, int tuplesPerBatch, int totalBatches) {
        Random random = new Random();
        int index;
        TxnEvent temp;
        for (int lop = 0; lop < totalBatches; lop++) {
            int start = lop * tuplesPerBatch;
            int end = (lop + 1) * tuplesPerBatch;
            for (int i = end - 1; i > start; i--) {
                index = start + random.nextInt(i - start + 1);
                temp = txnEvents.get(index);
                txnEvents.set(index, txnEvents.get(i));
                txnEvents.set(i, temp);
            }
        }
    }


    public void creates_Table(Configuration config) {
        RecordSchema s = AccountsScheme();
        db.createTable(s, "accounts");
        RecordSchema b = BookEntryScheme();
        db.createTable(b, "bookEntries");
        try {
            prepare_input_events(config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
