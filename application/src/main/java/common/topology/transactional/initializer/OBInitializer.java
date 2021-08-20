package common.topology.transactional.initializer;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.ob.AlertEvent;
import common.param.ob.BuyingEvent;
import common.param.ob.OBParam;
import common.param.ob.ToppingEvent;
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
import storage.datatype.LongDataBox;
import storage.table.RecordSchema;
import transaction.TableInitilizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;
import static common.Constants.Event_Path;
import static common.constants.OnlineBidingSystemConstants.Constant.*;
import static profiler.Metrics.NUM_ITEMS;
import static transaction.State.configure_store;
import static utils.PartitionHelper.getPartition_interval;

//import static xerial.jnuma.Numa.setLocalAlloc;
public class OBInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(OBInitializer.class);
    //triple decisions
    protected int[] triple_decision = new int[]{0, 0, 0, 0, 0, 0, 1, 2};//6:1:1 buy, alert, topping_handle.
    private int i = 0;

    public OBInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        configure_store(scale_factor, theta, tthread, NUM_ITEMS);
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
            insertItemRecords(key, 100);
        }
        if(enable_log) LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUMTasks) {
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
            insertItemRecords(key, 100, pid, spinlock);
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

    /**
     * 4 + 8 + 8
     * "INSERT INTO Table (key, value_list) VALUES (?, ?);"
     * initial account value_list is 0...?
     */
    private void insertItemRecords(int key, long value) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertItemRecords(int key, long value, int pid, SpinLock[] spinlock_) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));
        values.add(new LongDataBox(rnd.nextInt(MAX_Price)));//random price goods.
        values.add(new LongDataBox(value));//by default 100 qty of each good.
        SchemaRecord schemaRecord = new SchemaRecord(values);
        try {
            db.InsertRecord("goods", new TableRecord(schemaRecord, pid, spinlock_));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private RecordSchema Goods() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new LongDataBox());
        fieldNames.add("ID");//PK
        fieldNames.add("Price");
        fieldNames.add("Qty");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    /**
     * OB
     *
     * @param partition_id
     * @param bid_array
     * @param number_of_partitions
     * @param bid
     * @param rnd
     * @return
     */
    protected BuyingEvent randomBuyEvents(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {
        int pid = partition_id;
        OBParam param = new OBParam(NUM_ACCESSES_PER_BUY);
        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(NUM_ACCESSES_PER_BUY / (double) number_of_partitions);
        int counter = 0;
        randomkeys(pid, param, keys, access_per_partition, counter, NUM_ACCESSES_PER_BUY);
        assert !enable_states_partition || verify(keys, partition_id, number_of_partitions);
        return new BuyingEvent(param.keys(), rnd, partition_id, bid_array, bid, number_of_partitions);
    }

    protected AlertEvent randomAlertEvents(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {
        int pid = partition_id;
        int num_access = rnd.nextInt(NUM_ACCESSES_PER_ALERT) + 5;
        OBParam param = new OBParam(num_access);
        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(num_access / (double) number_of_partitions);
        int counter = 0;
        randomkeys(pid, param, keys, access_per_partition, counter, num_access);
        assert verify(keys, partition_id, number_of_partitions);
        return new AlertEvent(
                num_access,
                param.keys(),
                rnd,
                partition_id, bid_array, bid, number_of_partitions
        );
    }

    protected ToppingEvent randomToppingEvents(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {
        int pid = partition_id;
        int num_access = rnd.nextInt(NUM_ACCESSES_PER_TOP) + 5;
        OBParam param = new OBParam(num_access);
        Set keys = new HashSet();
        int access_per_partition = (int) Math.ceil(num_access / (double) number_of_partitions);
        int counter = 0;
        randomkeys(pid, param, keys, access_per_partition, counter, num_access);
        assert verify(keys, partition_id, number_of_partitions);
        return new ToppingEvent(
                num_access,
                param.keys(),
                rnd,
                partition_id, bid_array, bid, number_of_partitions
        );
    }

    protected int next_decision3() {
        int rt = triple_decision[i];
        i++;
        if (i == 8)
            i = 0;
        return rt;
    }

    @Override
    public Object create_new_event(int num_p, int bid) {
        int flag = next_decision3();
        if (flag == 0) {
            return randomBuyEvents(p, p_bid.clone(), num_p, bid, rnd);
        } else if (flag == 1) {
            return randomAlertEvents(p, p_bid.clone(), num_p, bid, rnd);//(AlertEvent) in.getValue(0);
        } else {
            return randomToppingEvents(p, p_bid.clone(), num_p, bid, rnd);//(AlertEvent) in.getValue(0);
        }
    }

    @Override
    public boolean Prepared(String file) throws IOException {
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition);
        return !Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file)));
    }

    @Override
    public void store(String file_name) throws IOException {
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition);
        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist
        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(event_path + OsUtils.OS_wrapper(file_name))));
        for (Object event : db.getEventManager().input_events) {
            StringBuilder sb = new StringBuilder();
            if (event instanceof BuyingEvent) {
                sb.append(((BuyingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((BuyingEvent) event).num_p());//3
                sb.append(split_exp);
                sb.append("BuyingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getItemId()));//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidPrice()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((BuyingEvent) event).getBidQty()));//7
            } else if (event instanceof AlertEvent) {
                sb.append(((AlertEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((AlertEvent) event).num_p());
                sb.append(split_exp);
                sb.append("AlertEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((AlertEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((AlertEvent) event).getAsk_price()));
            } else if (event instanceof ToppingEvent) {
                sb.append(((ToppingEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getPid());
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getBid_array()));
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).num_p());
                sb.append(split_exp);
                sb.append("ToppingEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((ToppingEvent) event).getNum_access());//5
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemId()));//6
                sb.append(split_exp);
                sb.append(Arrays.toString(((ToppingEvent) event).getItemTopUp()));
            }
            w.write(sb.toString() + "\n");
        }
        w.close();
    }

    public void creates_Table(Configuration config) {
        RecordSchema s = Goods();
        db.createTable(s, "goods");
        try {
            prepare_input_events("OB_Events");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
