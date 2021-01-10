package common.topology.transactional.initializer;
import benchmark.DataHolder;
import common.collections.Configuration;
import common.collections.OsUtils;
import datagenerator.DataGenerator;
import datagenerator.DataOperationChain;
import org.apache.storm.shade.org.eclipse.jetty.util.BlockingArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.Database;
import state_engine.DatabaseException;
import state_engine.common.SpinLock;
import state_engine.storage.SchemaRecord;
import state_engine.storage.TableRecord;
import state_engine.storage.datatype.DataBox;
import state_engine.storage.datatype.LongDataBox;
import state_engine.storage.datatype.StringDataBox;
import state_engine.storage.table.RecordSchema;
import state_engine.transaction.TableInitilizer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import static common.constants.StreamLedgerConstants.Constant.*;
import static state_engine.transaction.State.*;
//import static xerial.jnuma.Numa.setLocalAlloc;
public class SLInitializer extends TableInitilizer {

    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    private String dataRootPath;
    private int totalRecords;

    public SLInitializer(Database db, String dataRootPath, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        this.dataRootPath = dataRootPath;
        configure_store(scale_factor, theta, tthread, NUM_ACCOUNTS);
        totalRecords = config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches");
    }

    @Override
    public void loadDB(int thread_id, int NUM_TASK) {

        if(thread_id>0) // we are using single thread to load data.
            return;
        LOG.info("Thread:" + thread_id + " loading records...");

        int startingBalance = 1000000;
        String actTableKey = "accounts";
        String bookTableKey = "bookEntries";

        File file = new File(dataRootPath+"vertices_ids_range.txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String[] idsRangeInfo = reader.readLine().split(",");
            int accountIdsRange = Integer.parseInt(idsRangeInfo[0].split("=")[1]);
            int assetIdsRange = Integer.parseInt(idsRangeInfo[1].split("=")[1]);

            Random mRandomGeneratorForAccIds = new Random(12345678);
            Random mRandomGeneratorForAstIds = new Random(123456789);
            HashMap<Integer, Integer> mGeneratedAccountIds = new HashMap<>();
            HashMap<Integer, Integer> mGeneratedAssetIds = new HashMap<>();

            int range = 10 * totalRecords * 5;
            for(int lop=0; lop<accountIdsRange; lop++) {
//                int id = (int)Math.floor(Math.abs(mRandomGeneratorForAccIds.nextGaussian()/3.5)*range)%range;
//                while(mGeneratedAccountIds.containsKey(id))
//                    id = (int)Math.floor(Math.abs(mRandomGeneratorForAccIds.nextGaussian()/3.5)*range)%range;
                int id = mRandomGeneratorForAccIds.nextInt(10 * totalRecords * 5);
                while(mGeneratedAccountIds.containsKey(id))
                    id = mRandomGeneratorForAccIds.nextInt(10 * totalRecords * 5);
                mGeneratedAccountIds.put(id, null);

                String _key = String.format("%d", id);
                List<DataBox> values = new ArrayList<>();
                values.add(new StringDataBox(_key, _key.length()));
                values.add(new LongDataBox(startingBalance));
                TableRecord record = new TableRecord(new SchemaRecord(values));
                db.InsertRecord(actTableKey, record);

                if(lop%100000==0)
                    System.out.println(String.format("%d account records loaded...", lop));
            }

            for(int lop=0; lop<assetIdsRange; lop++) {
//                int id = (int)Math.floor(Math.abs(mRandomGeneratorForAstIds.nextGaussian()/3.5)*range)%range;
//                while(mGeneratedAssetIds.containsKey(id))
//                    id = (int)Math.floor(Math.abs(mRandomGeneratorForAstIds.nextGaussian()/3.5)*range)%range;
                int id = mRandomGeneratorForAstIds.nextInt(10 * totalRecords * 5);
                while(mGeneratedAssetIds.containsKey(id))
                    id = mRandomGeneratorForAstIds.nextInt(10 * totalRecords * 5);
                mGeneratedAssetIds.put(id, null);

                String _key = String.format("%d", id);
                List<DataBox> values = new ArrayList<>();
                values.add(new StringDataBox(_key, _key.length()));
                values.add(new LongDataBox(startingBalance));
                TableRecord record = new TableRecord(new SchemaRecord(values));
                db.InsertRecord(bookTableKey, record);

                if(lop%100000==0)
                    System.out.println(String.format("%d asset records loaded...", lop));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("Thread:" + thread_id + " finished loading records...");
        System.gc();
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUM_TASK) {
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
        return prefix + String.valueOf(key);
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
    public boolean Prepared(String file) throws IOException {
        return Files.exists(Paths.get(dataRootPath));
    }

    @Override
    public void store(String file_name) throws IOException {
        // User Data Generator here,
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
            prepare_input_events("SL_Events", config.getInt("totalEventsPerBatch")*config.getInt("numberOfBatches"), false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
