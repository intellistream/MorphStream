package intellistream.morphstream.examples.tsp.tollprocessing.util;

import intellistream.morphstream.engine.txn.DataHolder;
import intellistream.morphstream.examples.utils.datagen.DataGenerator;
import intellistream.morphstream.examples.utils.datagen.DataGeneratorConfig;
import intellistream.morphstream.examples.tsp.tollprocessing.events.TPTPGDynamicDataGenerator;
import intellistream.morphstream.examples.utils.datagen.DynamicDataGeneratorConfig;
import intellistream.morphstream.examples.tsp.tollprocessing.util.datatype.PositionReport;
import intellistream.morphstream.examples.tsp.tollprocessing.events.TPTxnEvent;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.db.Database;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.datatype.DoubleDataBox;
import intellistream.morphstream.engine.txn.storage.datatype.HashSetDataBox;
import intellistream.morphstream.engine.txn.storage.datatype.StringDataBox;
import intellistream.morphstream.engine.txn.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.transaction.TableInitilizer;
import intellistream.morphstream.util.AppConfig;
import intellistream.morphstream.util.OsUtils;
import intellistream.morphstream.util.randomNumberGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.engine.txn.transaction.State.configure_store;

public class TPInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TPInitializer.class);
    private final int numberOfStates;
    private final DataGeneratorConfig dataConfig;
    private final int partitionOffset;
    private final int NUM_ACCESS;
    private final int Transaction_Length;
    private String dataRootPath;
    private DataGenerator dataGenerator;

    public TPInitializer(Database db, int numberOfStates, double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
        floor_interval = (int) Math.floor(numberOfStates / (double) tthread);//NUM_ITEMS / tthread;
        this.dataRootPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("inputs");
        this.partitionOffset = numberOfStates / tthread;
        this.NUM_ACCESS = config.getInt("NUM_ACCESS");
        this.Transaction_Length = config.getInt("Transaction_Length");
        this.numberOfStates = numberOfStates;
        configure_store(theta, tthread, numberOfStates);
        createTPGGenerator(config);
        dataConfig = dataGenerator.getDataConfig();
    }

    protected void createTPGGenerator(Configuration config) {
        DynamicDataGeneratorConfig dynamicDataGeneratorConfig = new DynamicDataGeneratorConfig();
        dynamicDataGeneratorConfig.initialize(config);
        configurePath(dynamicDataGeneratorConfig);
        dataGenerator = new TPTPGDynamicDataGenerator(dynamicDataGeneratorConfig);
    }

    private void configurePath(DynamicDataGeneratorConfig dynamicDataGeneratorConfig) {
        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes;
            bytes = digest.digest(String.format("%d_%d_%d_%s_%s",
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
            String _key = String.valueOf(key);
            insertSpeedRecord(_key, 0, pid, spinlock, thread_id);
            insertCntRecord(_key, 0, pid, spinlock, thread_id);
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
            String _key = String.valueOf(key);
            insertSpeedRecord(_key, 0, thread_id);
            insertCntRecord(_key, thread_id);
        }
        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    private void insertCntRecord(String key, int value, int pid, SpinLock[] spinlock, int partition_id) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_cnt", new TableRecord(schemaRecord, pid, spinlock), partition_id);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertSpeedRecord(String key, int value, int pid, SpinLock[] spinlock, int partition_id) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new DoubleDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_speed", new TableRecord(schemaRecord, pid, spinlock), partition_id);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertCntRecord(String key, int partition_id) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_cnt", new TableRecord(schemaRecord, this.tthread), partition_id);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertSpeedRecord(String key, int value, int partition_id) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new DoubleDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_speed", new TableRecord(schemaRecord, this.tthread), partition_id);
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private RecordSchema SpeedScheme() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new DoubleDataBox());
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema CntScheme() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new HashSetDataBox());
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
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
        long[] p_bids = new long[tthread];
        if (file.exists()) {
            if (enable_log) LOG.info("Reading transfer events...");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            loadTollProcessingEvents(reader, totalEvents, shufflingActive, p_bids);
            reader.close();
        }
    }

    private void loadTollProcessingEvents(BufferedReader reader, int totalEvents, boolean shufflingActive, long[] p_bids) throws IOException {
        String txn = reader.readLine();
        int count = 0;
        while (txn != null) {
            TPTxnEvent event;
            PositionReport positionReport;
            String[] split = txn.split(",");
            if (split[split.length - 1].equals("true")) {
                positionReport = new PositionReport((short) 0,
                        randomNumberGenerator.generate(1, 100),
                        200,
                        randomNumberGenerator.generate(1, 4),
                        (short) randomNumberGenerator.generate(1, 4),
                        (short) randomNumberGenerator.generate(1, 1),
                        Integer.parseInt(split[1]),
                        randomNumberGenerator.generate(1, 100));
            } else {
                positionReport = new PositionReport((short) 0,
                        randomNumberGenerator.generate(1, 100),
                        randomNumberGenerator.generate(60, 180),
                        randomNumberGenerator.generate(1, 4),
                        (short) randomNumberGenerator.generate(1, 4),
                        (short) randomNumberGenerator.generate(1, 1),
                        Integer.parseInt(split[1]),
                        randomNumberGenerator.generate(1, 100));
            }
            event = new TPTxnEvent(positionReport, dataConfig.getTotalThreads(), Long.parseLong(split[0]));
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
    public void store(String file_name) {
    }

    @Override
    public List<String> getTranToDecisionConf() {
        return dataGenerator.getTranToDecisionConf();
    }

    protected String getConfigKey(String template) {
        return String.format(template, "tptxn");//TODO: make it flexible in future.
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = SpeedScheme();
        db.createTable(s, "segment_speed", config.getInt("tthread"), config.getInt("NUM_ITEMS"));
        RecordSchema b = CntScheme();
        db.createTable(b, "segment_cnt", config.getInt("tthread"), config.getInt("NUM_ITEMS"));
        try {
            prepare_input_events(config.getInt("totalEvents"));
            if (getTranToDecisionConf() != null && getTranToDecisionConf().size() != 0) {
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
