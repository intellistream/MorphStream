package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.apps.TP.TPTPGDynamicDataGenerator;
import benchmark.dynamicWorkloadGenerator.DynamicDataGeneratorConfig;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.datatype.PositionReport;
import common.param.TxnEvent;
import common.param.lr.LREvent;
import common.tools.randomNumberGenerator;
import db.Database;
import db.DatabaseException;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.SchedulerContext;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.HashSetDataBox;
import storage.datatype.StringDataBox;
import storage.table.RecordSchema;
import transaction.TableInitilizer;
import utils.AppConfig;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static common.CONTROL.enable_log;
import static common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static profiler.Metrics.NUM_ITEMS;
import static transaction.State.configure_store;
import static utils.PartitionHelper.getPartition_interval;

public class TPInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TPInitializer.class);
    private final int numberOfStates;
    private String dataRootPath;
    private DataGenerator dataGenerator;
    private final DataGeneratorConfig dataConfig;
    private final int partitionOffset;
    private final int NUM_ACCESS;
    private final int Transaction_Length;
    public TPInitializer(Database db,int numberOfStates, double theta, int tthread, Configuration config) {
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
        int partition_interval = (int) Math.ceil(numberOfStates / (double) NUMTasks);;
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
            insertSpeedRecord(_key, 0, pid, spinlock);
            insertCntRecord(_key, 0, pid, spinlock);
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
            insertSpeedRecord(_key, 0);
            insertCntRecord(_key);
        }
        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    private void insertCntRecord(String key, int value, int pid, SpinLock[] spinlock) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_cnt", new TableRecord(schemaRecord, pid, spinlock));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertSpeedRecord(String key, int value, int pid, SpinLock[] spinlock) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new DoubleDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_speed", new TableRecord(schemaRecord, pid, spinlock));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertCntRecord(String key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new HashSetDataBox());
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_cnt", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertSpeedRecord(String key, int value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(key, key.length()));
        values.add(new DoubleDataBox(value));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("segment_speed", new TableRecord(schemaRecord));
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
            LREvent event;
            PositionReport positionReport;
            String[] split = txn.split(",");
            if (split[split.length-1].equals("true")) {
                positionReport = new PositionReport((short) 0,
                        randomNumberGenerator.generate(1,100),
                        200,
                        randomNumberGenerator.generate(1,4),
                        (short)randomNumberGenerator.generate(1,4),
                        (short)randomNumberGenerator.generate(1,1),
                        Integer.parseInt(split[1]),
                        randomNumberGenerator.generate(1,100));
            } else {
                positionReport = new PositionReport((short) 0,
                        randomNumberGenerator.generate(1,100),
                        randomNumberGenerator.generate(60,180),
                        randomNumberGenerator.generate(1,4),
                        (short)randomNumberGenerator.generate(1,4),
                        (short)randomNumberGenerator.generate(1,1),
                        Integer.parseInt(split[1]),
                        randomNumberGenerator.generate(1,100));
            }
            event = new LREvent(positionReport,dataConfig.getTotalThreads(),Long.parseLong(split[0]));
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
        db.createTable(s, "segment_speed");
        RecordSchema b = CntScheme();
        db.createTable(b, "segment_cnt");
        try {
            prepare_input_events(config.getInt("totalEvents"));
            if (getTranToDecisionConf() != null && getTranToDecisionConf().size() != 0){
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
