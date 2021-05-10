package common.topology.transactional.initializer;
import benchmark.DataHolder;
import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import common.SpinLock;
import common.collections.Configuration;
import common.collections.OsUtils;
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

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import static common.constants.StreamLedgerConstants.Constant.*;
import static transaction.State.configure_store;
//import static xerial.jnuma.Numa.setLocalAlloc;
public class SLInitializer extends TableInitilizer {

    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    private String dataRootPath;
    private final int totalRecords;
    private final String idsGenType;
    private DataGenerator mDataGenerator;


    private int startingBalance = 1000000;
    private String actTableKey = "accounts";
    private String bookTableKey = "bookEntries";
    private int mPartitionOffset;
    public SLInitializer(Database db, String dataRootPath, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        this.dataRootPath = dataRootPath;
        configure_store(scale_factor, theta, tthread, NUM_ACCOUNTS);
        totalRecords = config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches");
        idsGenType = config.getString("idGenType");
        this.mPartitionOffset = (totalRecords * 5)/tthread;

        createDataGenerator(config);

    }

    protected void createDataGenerator(Configuration config) {

        DataGeneratorConfig dataConfig = new DataGeneratorConfig();
        dataConfig.initialize(config);

        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(
                            digest.digest(
                                    String.format("%d_%s", dataConfig.tuplesPerBatch * dataConfig.totalBatches,
                                            Arrays.toString(dataConfig.dependenciesDistributionForLevels))
                                            .getBytes(StandardCharsets.UTF_8))));
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataConfig.rootPath += OsUtils.OS_wrapper(subFolder);
        dataConfig.idsPath += OsUtils.OS_wrapper(subFolder);
        this.dataRootPath += OsUtils.OS_wrapper(subFolder);

        mDataGenerator = new DataGenerator(dataConfig);
    }

    @Override
    public void loadDB(int thread_id, int NUM_TASK) {
        loadDB(thread_id, null, NUM_TASK);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUM_TASK) {

        int[] maxId = readRecordMaximumIds(); // read maximum possible number of ids.

        // load account records in to the db.
        LOG.info("Thread:" + thread_id + " loading account records...");
        Random mRandomGeneratorForAccIds = new Random(12345678); // Imp: DataGenerator uses the same seed.
        HashMap<Long, Integer> mGeneratedAccountIds = new HashMap<>();
        for (int idNum = 0; idNum <= maxId[0]; idNum++) {

            long id = getNextId(mRandomGeneratorForAccIds) + (thread_id*mPartitionOffset); // each thread loads data for the corresponding partition.
            id *= 10; //scaling the id.
            for(int iter = 0; iter < 10; iter++) {   // fill gap between scaled ids.
                if(mGeneratedAccountIds.containsKey(id + iter))
                    continue;
                mGeneratedAccountIds.put(id + iter, null);
                insertAccountRecord(id + iter, thread_id, spinlock);
            }
        }

        // load asset records in to the db.
        LOG.info("Thread:" + thread_id + " loading asset records...");
        Random mRandomGeneratorForAstIds = new Random(123456789); // Imp: DataGenerator uses the same seed.
        HashMap<Long, Integer> mGeneratedAssetIds = new HashMap<>();
        for (int idNum = 0; idNum <= maxId[1]; idNum++) {

            long id = getNextId(mRandomGeneratorForAstIds) + (thread_id*mPartitionOffset);
            id *= 10;
            for(int iter = 0; iter < 10; iter++) {
                if(mGeneratedAssetIds.containsKey(id + iter))
                    continue;
                mGeneratedAssetIds.put(id + iter, null);
                insertAssetRecord(id + iter, thread_id, spinlock);
            }
        }

        LOG.info("Thread:" + thread_id + " finished loading records...");
        System.gc();
    }

    private int[] readRecordMaximumIds() {

        File file = new File(dataRootPath  + OsUtils.OS_wrapper( "vertices_ids_range.txt"));
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

    private int getNextId(Random random) {
        int id = 0;
        if (idsGenType.equals("uniform")) {
            id = random.nextInt(mPartitionOffset);
        } else if (idsGenType.equals("normal")) {
            id = (int) Math.floor(Math.abs(random.nextGaussian() / 3.5) * mPartitionOffset) % mPartitionOffset;
        }
        return id;
    }

    private void insertAccountRecord(long id, int thread_id, SpinLock[] spinlock) {
        String _key = String.format("%d", id);
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(_key, _key.length()));
        values.add(new LongDataBox(startingBalance));
        TableRecord record = null;
        if(spinlock!=null)
            record = new TableRecord(new SchemaRecord(values), thread_id, spinlock);
        else
            record = new TableRecord(new SchemaRecord(values));
        try {
            db.InsertRecord(actTableKey, record);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertAssetRecord(long id, int thread_id, SpinLock[] spinlock) {
        String _key = String.format("%d", id);
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(_key, _key.length()));
        values.add(new LongDataBox(startingBalance));
        TableRecord record = null;
        if(spinlock!=null)
            record = new TableRecord(new SchemaRecord(values), thread_id, spinlock);
        else
            record = new TableRecord(new SchemaRecord(values));
        try {
            db.InsertRecord(bookTableKey, record);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }
    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAccountRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("accounts", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }
    private void insertAccountRecord(String key, long value, int pid, SpinLock[] spinlock_) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);

        try {
            db.InsertRecord("accounts", new TableRecord(schemaRecord, pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }
    private SchemaRecord AssetRecord(String key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        return new SchemaRecord(values);
    }
    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAssetRecord(String key, long value) {
        try {
            TableRecord record = new TableRecord(AssetRecord(key, value));
            db.InsertRecord("bookEntries", record);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }
    private void insertAssetRecord(String key, long value, int pid, SpinLock[] spinlock_) {
        try {
            db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value), pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }
    //    private String rightpad(String text, int length) {
//        return String.format("%-" + length + "." + length + "s", text);
//    }
//
    private String GenerateKey(String prefix, int key) {
//        return rightpad(prefix + String.valueOf(key), VALUE_LEN);
        return prefix + key;
    }
    /**
     * TODO: be aware, scale_factor is not in use now.
     *
     * @param scale_factor
     * @param theta
     * @param partition_interval
     * @param spinlock_
     */
    public void loadData_Central(double scale_factor, double theta, int partition_interval, SpinLock[] spinlock_) {
        int elements = (int) (NUM_ACCOUNTS * scale_factor);
        int elements_per_socket;
//        setLocalAlloc();
        if (OsUtils.isMac())
            elements_per_socket = elements;
        else
            elements_per_socket = elements / 4;
        int i = 0;
        for (int key = 0; key < elements; key++) {
            int pid = get_pid(partition_interval, key);
            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0, pid, spinlock_);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0, pid, spinlock_);
            i++;
        }
    }
    @Override
    public void loadData_Central(double scale_factor, double theta) {
        int elements = (int) (NUM_ACCOUNTS * scale_factor);
//        setLocalAlloc();
        int i = 0;
        for (int key = 0; key < elements; key++) {
            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0);
            i++;
        }
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
    public boolean Prepared(String fileN) throws IOException {

        int tuplesPerBatch = mDataGenerator.getDataConfig().tuplesPerBatch;
        int totalBatches = mDataGenerator.getDataConfig().totalBatches;
        int numberOfLevels = mDataGenerator.getDataConfig().numberOfDLevels;
        int tt = mDataGenerator.getDataConfig().totalThreads;
        boolean shufflingActive = mDataGenerator.getDataConfig().shufflingActive;
        String folder = mDataGenerator.getDataConfig().rootPath;

        String statsFolderPattern = mDataGenerator.getDataConfig().idsPath
                + OsUtils.osWrapperPostFix("stats")
                + OsUtils.osWrapperPostFix("scheduler = %s")
                + OsUtils.osWrapperPostFix("depth = %d")
                + OsUtils.osWrapperPostFix("threads = %d")
                + OsUtils.osWrapperPostFix("total_batches = %d")
                + OsUtils.osWrapperPostFix("events_per_batch = %d");

        String statsFolderPath = String.format(statsFolderPattern, mDataGenerator.getDataConfig().scheduler, numberOfLevels, tt, totalBatches, tuplesPerBatch);
        File file = new File(statsFolderPath + String.format("iteration_0.csv"));
        if (!file.exists()) {
            mDataGenerator.GenerateData();
            mDataGenerator = null;
        }
        loadTransactionEvents(tuplesPerBatch, totalBatches, shufflingActive, folder);
        return true;
    }

    @Override
    public void store(String file_name) throws IOException {

    }

    protected void loadTransactionEvents(int tuplesPerBatch, int totalBatches, boolean shufflingActive, String folder) {

        if (DataHolder.events == null) {
            int numberOfEvents = tuplesPerBatch * totalBatches;
            int mPartitionOffset = (10 * numberOfEvents * 5)/tthread;
            DataHolder.events = new TransactionEvent[numberOfEvents];
            File file = new File(folder + "transactions.txt");
            if (file.exists()) {
                LOG.info(String.format("Reading transactions..."));
                try {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
                    String txn = reader.readLine();
                    int count = 0;
                    int p_bids[] = new int[tthread];
                    while (txn != null) {
                        String[] split = txn.split(",");
                        int npid = (int) (Long.valueOf(split[1])/mPartitionOffset);
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
                        for(int x = 0; x<4; x++)
                            p_bids[(npid+x)%tthread]++;
                        DataHolder.events[count] = event;
                        count++;
                        if (count % 100000 == 0)
                            LOG.info(String.format("%d transactions read...", count));
                        txn = reader.readLine();
                    }
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                LOG.info(String.format("Done reading transactions..."));

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
