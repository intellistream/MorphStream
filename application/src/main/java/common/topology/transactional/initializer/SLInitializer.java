package common.topology.transactional.initializer;
import common.collections.Configuration;
import common.collections.OsUtils;
import datagenerator.DataGenerator;
import datagenerator.DataOperationChain;
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


import static common.constants.StreamLedgerConstants.Constant.*;
import static state_engine.transaction.State.*;
//import static xerial.jnuma.Numa.setLocalAlloc;
public class SLInitializer extends TableInitilizer {

    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    private String dataRootPath;

    public SLInitializer(Database db, String dataRootPath, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        this.dataRootPath = dataRootPath;
        configure_store(scale_factor, theta, tthread, NUM_ACCOUNTS);
    }


    public static long totalTimeStart = 0;
    public static long totalTime = 0;
    public static long insertTimeStart = 0;
    public static long insertTime = 0;
    public static long fileReadTimeStart = 0;
    public static long fileReadTime = 0;
    public static long memAllocTimeStart = 0;
    public static long memAllocTime = 0;

    @Override
    public void loadDB(int thread_id, int NUM_TASK) {
        if(thread_id>0) // we are using single thread to load data.
            return;
        LOG.info("Thread:" + thread_id + " loading records...");
        File file = new File(dataRootPath+"dependency_vertices.csv");
        System.out.println(String.format("SLinit folder path %s.", dataRootPath+"dependency_vertices.csv"));
        try {
            int startingBalance = 100000;
            int records = 0;
            String actTableKey = "accounts";
            String bookTableKey = "bookEntries";
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String id = reader.readLine();

            while(id!=null) {
                String _key = id.substring(4);

                List<DataBox> values = new ArrayList<>();
                values.add(new StringDataBox(_key, _key.length()));
                values.add(new LongDataBox(startingBalance));
                TableRecord record = new TableRecord(new SchemaRecord(values));

                if(id.contains("act_"))
                    db.InsertRecord(actTableKey, record);
                else
                    db.InsertRecord(bookTableKey, record);
                id = reader.readLine();

                records++;
                if(records%100000==0)
                    LOG.info("Thread:" + records + " records loaded...");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.info("Thread:" + thread_id + " finished loading records...");
    }

    // Aqif: Disk IOs are the biggest bootleneck to process large datasets. Thus generating and processing in-memory.
//    @Override
//    public void loadDB(int thread_id, int NUM_TASK) {
//        if(thread_id>0) // we are using single thread to load data.
//            return;
//
//        LOG.info("Thread:" + thread_id + " loading records...");
//        int startingBalance = 100000;
//        int records = 0;
////        File file = new File(dataRootPath+"dependency_vertices.csv");
////        System.out.println(String.format("SLinit folder path %s.", dataRootPath+"dependency_vertices.csv"));
//
//        String actTableKey = "accounts";
//        String bookTableKey = "bookEntries";
//
//        String id = null;
//
//        for(ArrayList<DataOperationChain> operationChains: DataGenerator.mAccountOperationChainsByLevel.values()) {
//            for(int lop=operationChains.size()-1; lop>=0; lop--) {
//                records++;
//                id = operationChains.get(lop).getStateId();
//                String _key = id.substring(4);
//                List<DataBox> values = new ArrayList<>();
//                values.add(new StringDataBox(_key, _key.length()));
//                values.add(new LongDataBox(startingBalance));
//                TableRecord record = new TableRecord(new SchemaRecord(values));
//                try {
//                    db.InsertRecord(actTableKey, record);
//                } catch (DatabaseException e) {
//                    e.printStackTrace();
//                }
//                if(records%100000==0) {
//                    LOG.info("Thread:" + records + " records loaded...");
//                }
//            }
//        }
//
//
//        for(ArrayList<DataOperationChain> operationChains: DataGenerator.mAssetsOperationChainsByLevel.values()) {
//            for(int lop=operationChains.size()-1; lop>=0; lop--) {
//                records++;
//                id = operationChains.get(lop).getStateId();
//                String _key = id.substring(4);
//                List<DataBox> values = new ArrayList<>();
//                values.add(new StringDataBox(_key, _key.length()));
//                values.add(new LongDataBox(startingBalance));
//                TableRecord record = new TableRecord(new SchemaRecord(values));
//                try {
//                    db.InsertRecord(actTableKey, record);
//                } catch (DatabaseException e) {
//                    e.printStackTrace();
//                }
//                if(records%100000==0) {
//                    LOG.info("Thread:" + records + " records loaded...");
//                }
//            }
//        }
//
//        LOG.info("Thread:" + thread_id + " finished loading records...");
//    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUM_TASK) {
    }
    /**
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertAccountRecord(String key, long value) {
        memAllocTimeStart = System.nanoTime();
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new LongDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        memAllocTime += System.nanoTime() - memAllocTimeStart;
        try {
            insertTimeStart = System.nanoTime();
            db.InsertRecord("accounts", new TableRecord(schemaRecord));
            insertTime += System.nanoTime() - insertTimeStart;
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
            memAllocTimeStart = System.nanoTime();
            TableRecord record = new TableRecord(AssetRecord(key, value));
            memAllocTime += System.nanoTime() - memAllocTimeStart;
            insertTimeStart = System.nanoTime();
            db.InsertRecord("bookEntries", record);
            insertTime += System.nanoTime() - insertTimeStart;
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
