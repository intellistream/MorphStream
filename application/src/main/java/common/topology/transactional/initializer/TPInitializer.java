package common.topology.transactional.initializer;

import common.collections.Configuration;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static common.CONTROL.enable_log;
import static common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static utils.PartitionHelper.getPartition_interval;

public class TPInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TPInitializer.class);

    public TPInitializer(Database db, double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUMTasks) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUMTasks - 1) {//last executor need to handle left-over
            right_bound = NUM_SEGMENTS;
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
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUMTasks - 1) {//last executor need to handle left-over
            right_bound = NUM_SEGMENTS;
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
        return true;
    }


    @Override
    protected void Load() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void store(String file_name) {
    }

    protected String getConfigKey(String template) {
        return String.format(template, "tptxn");//TODO: make it flexible in future.
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = SpeedScheme();
        db.createTable(s, "segment_speed");
        RecordSchema b = CntScheme();
        db.createTable(b, "segment_cnt");
    }
}
