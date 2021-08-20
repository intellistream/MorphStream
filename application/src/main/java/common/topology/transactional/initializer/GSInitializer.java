package common.topology.transactional.initializer;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.MicroParam;
import common.param.mb.MicroEvent;
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
import storage.datatype.StringDataBox;
import storage.table.RecordSchema;
import transaction.TableInitilizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;
import static common.Constants.Event_Path;
import static common.constants.GrepSumConstants.Constant.VALUE_LEN;
import static common.param.mb.MicroEvent.GenerateValue;
import static profiler.Metrics.NUM_ACCESSES;
import static profiler.Metrics.NUM_ITEMS;
import static transaction.State.configure_store;
import static utils.PartitionHelper.getPartition_interval;

public class GSInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(GSInitializer.class);
    //different R-W ratio.
    //just enable one of the decision array
    protected transient boolean[] read_decision;
    int i = 0;

    public GSInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        floor_interval = (int) Math.floor(NUM_ITEMS / (double) tthread);//NUM_ITEMS / tthread;
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
        if (ratio_of_read == 0) {
            read_decision = new boolean[]{false, false, false, false, false, false, false, false};// all write.
        } else if (ratio_of_read == 0.25) {
            read_decision = new boolean[]{false, false, false, false, false, false, true, true};//75% W, 25% R.
        } else if (ratio_of_read == 0.5) {
            read_decision = new boolean[]{false, false, false, false, true, true, true, true};//equal r-w ratio.
        } else if (ratio_of_read == 0.75) {
            read_decision = new boolean[]{false, false, true, true, true, true, true, true};//25% W, 75% R.
        } else if (ratio_of_read == 1) {
            read_decision = new boolean[]{true, true, true, true, true, true, true, true};// all read.
        } else {
            System.exit(-1);
        }
        if(enable_log) LOG.info("ratio_of_read: " + ratio_of_read + "\tREAD DECISIONS: " + Arrays.toString(read_decision));
        configure_store(scale_factor, theta, tthread, NUM_ITEMS);
    }

    /**
     * "INSERT INTO MicroTable (key, value_list) VALUES (?, ?);"
     */
    private void insertMicroRecord(int key, String value, int pid, SpinLock[] spinlock_) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new StringDataBox(value, value.length()));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("MicroTable", new TableRecord(schemaRecord, pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertMicroRecord(int key, String value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new StringDataBox(value, value.length()));
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("MicroTable", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void loadDB(int thread_id, int NUMTasks) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUMTasks - 1) {//last executor need to handle left-over
            right_bound = NUM_ITEMS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value);
        }
        if(enable_log) LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock_, int NUMTasks) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUMTasks - 1) {//last executor need to handle left-over
            right_bound = NUM_ITEMS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);
            String value = GenerateValue(key);
            assert value.length() == VALUE_LEN;
            insertMicroRecord(key, value, pid, spinlock_);
        }
        if(enable_log) LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
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
    public boolean Generate() throws IOException {
//        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
//        this.number_partitions = Math.min(tthread, config.getInt("number_partitions"));
//        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
//        String event_path = Event_Path
//                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition)
//                + OsUtils.OS_wrapper("NUM_EVENTS=" + config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches"))
//                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + ratio_of_multi_partition)
//                + OsUtils.OS_wrapper("number_partitions=" + number_partitions)
//                + OsUtils.OS_wrapper("ratio_of_read=" + ratio_of_read)
//                + OsUtils.OS_wrapper("NUM_ACCESSES=" + NUM_ACCESSES)
//                + OsUtils.OS_wrapper("theta=" + theta)
//                + OsUtils.OS_wrapper("NUM_ITEMS=" + NUM_ITEMS);
//        return !Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file)));
        throw new UnsupportedOperationException();
    }

    @Override
    protected void Load() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void store(String file_name) throws IOException {
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
        this.number_partitions = Math.min(tthread, config.getInt("number_partitions"));
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition)
                + OsUtils.OS_wrapper("NUM_EVENTS=" + config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches"))
                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + ratio_of_multi_partition)
                + OsUtils.OS_wrapper("number_partitions=" + number_partitions)
                + OsUtils.OS_wrapper("ratio_of_read=" + ratio_of_read)
                + OsUtils.OS_wrapper("NUM_ACCESSES=" + NUM_ACCESSES)
                + OsUtils.OS_wrapper("theta=" + theta)
                + OsUtils.OS_wrapper("NUM_ITEMS=" + NUM_ITEMS);
        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist
        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(event_path + OsUtils.OS_wrapper(file_name))));
        for (Object event : db.getEventManager().input_events) {
            MicroEvent microEvent = (MicroEvent) event;
            String sb =
                    microEvent.getBid() +//0 -- bid
                            split_exp +
                            microEvent.getPid() +//1
                            split_exp +
                            Arrays.toString(microEvent.getBid_array()) +//2
                            split_exp +
                            microEvent.num_p() +//3 num of p
                            split_exp +
                            "MicroEvent" +//4 input_event types.
                            split_exp +
                            Arrays.toString(microEvent.getKeys()) +//5 keys
                            split_exp +
                            microEvent.READ_EVENT()//6
                    ;
            w.write(sb
                    + "\n");
        }
        w.close();
    }

    protected boolean next_read_decision() {
        boolean rt = read_decision[i];
        i++;
        if (i == 8)
            i = 0;
        return rt;
    }

    /**
     * Generate events according to the given parition_id.
     *
     * @param partition_id
     * @param bid_array
     * @param bid
     * @param flag
     * @return
     */
    protected MicroEvent generateEvent(int partition_id,
                                       long[] bid_array, int number_of_partitions, long bid, boolean flag) {
        int pid = partition_id;
        MicroParam param = new MicroParam(NUM_ACCESSES);
        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(NUM_ACCESSES / (double) number_of_partitions);
        int counter = 0;
        randomkeys(pid, param, keys, access_per_partition, counter, NUM_ACCESSES);
        assert !enable_states_partition || verify(keys, partition_id, number_of_partitions);
        return new MicroEvent(
                param.keys(),
                flag,
                NUM_ACCESSES,
                bid,
                partition_id,
                bid_array,
                number_of_partitions
        );
    }

    private RecordSchema MicroTableSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new StringDataBox());
        fieldNames.add("Key");//PK
        fieldNames.add("Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = MicroTableSchema();
        db.createTable(s, "MicroTable");
        try {
            prepare_input_events(config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
