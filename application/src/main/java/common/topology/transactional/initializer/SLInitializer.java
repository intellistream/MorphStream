package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.SpecialDataGenerator;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGenerator;
import benchmark.datagenerator.apps.SL.OCTxnGenerator.LayeredOCDataGeneratorConfig;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.TPGDataGenerator;
import benchmark.datagenerator.apps.SL.TPGTxnGenerator.TPGDataGeneratorConfig;
import common.collections.Configuration;
import common.collections.OsUtils;
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

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static common.CONTROL.enable_log;
import static transaction.State.configure_store;

public class SLInitializer extends TableInitilizer {

    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    private final int totalRecords;
    private final String idsGenType;
    private final int numberOfStates;
    private final int startingBalance = 1000000;
    private final int partitionOffset;
    private String dataRootPath;
    private SpecialDataGenerator dataGenerator;

    public SLInitializer(Database db, String dataRootPath, int numberOfStates, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        this.numberOfStates = numberOfStates;
        this.dataRootPath = dataRootPath;
        configure_store(scale_factor, theta, tthread, this.numberOfStates);
        totalRecords = config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches");
        idsGenType = config.getString("idGenType");
        this.partitionOffset = this.numberOfStates / tthread;

//        Controller.setExec(tthread);

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

    private void configurePath(DataGeneratorConfig dataConfig) {
        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(
                            digest.digest(
                                    String.format("%d_%d",
                                                    dataConfig.getTotalThreads(),
                                                    dataConfig.getTuplesPerBatch() * dataConfig.getTotalBatches())
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
        if(enable_log) LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
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
            insertAssetRecord(_key, startingBalance, pid, spinlock);
        }
        if(enable_log) LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
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
    public boolean Prepared(String fileN) {

        int tuplesPerBatch = dataGenerator.getDataConfig().getTuplesPerBatch();
        int totalBatches = dataGenerator.getDataConfig().getTotalBatches();
        boolean shufflingActive = dataGenerator.getDataConfig().getShufflingActive();
        String folder = dataGenerator.getDataConfig().getRootPath();

        dataGenerator.prepareForExecution();

        loadTransactionEvents(tuplesPerBatch, totalBatches, shufflingActive, folder);
        return true;
    }

    @Override
    public void store(String file_name) throws IOException {

    }

    protected void loadTransactionEvents(int tuplesPerBatch, int totalBatches, boolean shufflingActive, String folder) {

        if (DataHolder.events == null) {
            DataHolder.events = new TransactionEvent[totalRecords];
            File file = new File(folder + "transactions.txt");
            if (file.exists()) {
                if(enable_log) LOG.info(String.format("Reading transactions..."));
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    String txn = reader.readLine();
                    int count = 0;
                    int p_bids[] = new int[tthread];
                    while (txn != null) {
                        String[] split = txn.split(",");
                        int npid = (int) (Long.valueOf(split[1]) / partitionOffset);
                        TransactionEvent event = new TransactionEvent(
                                Integer.parseInt(split[0]), //bid
                                npid, //pid
                                Arrays.toString(p_bids), //bid_array
                                4,//num_of_partition
                                split[1],//getSourceAccountId
                                split[2],//getSourceBookEntryId
                                split[3],//getTargetAccountId
                                split[4],//getTargetBookEntryId
                                100,  //getAccountTransfer
                                100  //getBookEntryTransfer
                        );
                        for (int x = 0; x < 4; x++)
                            p_bids[(npid + x) % tthread]++;
                        DataHolder.events[count] = event;
                        count++;
                        if (count % 100000 == 0)
                            if(enable_log) LOG.info(String.format("%d transactions read...", count));
                        txn = reader.readLine();
                    }
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if(enable_log) LOG.info(String.format("Done reading transactions..."));

                if (shufflingActive) {
                    Random random = new Random();
                    int index;
                    TransactionEvent temp;
                    for (int lop = 0; lop < totalBatches; lop++) {
                        int start = lop * tuplesPerBatch;
                        int end = (lop + 1) * tuplesPerBatch;

                        for (int i = end - 1; i > start; i--) {
                            index = start + random.nextInt(i - start + 1);
                            temp = DataHolder.events[index];
                            DataHolder.events[index] = DataHolder.events[i];
                            DataHolder.events[i] = temp;
                        }
                    }
                }
            }
        }
    }

    @Override
    public Object create_new_event(int num_p, int bid) {
        return null;
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = AccountsScheme();
        db.createTable(s, "accounts");
        RecordSchema b = BookEntryScheme();
        db.createTable(b, "bookEntries");
        try {
            prepare_input_events("SL_Events", config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
