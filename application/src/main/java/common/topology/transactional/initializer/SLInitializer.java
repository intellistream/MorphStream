package common.topology.transactional.initializer;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

import static common.CONTROL.NUM_EVENTS;
import static common.CONTROL.enable_states_partition;
import static common.Constants.Event_Path;
import static common.constants.StreamLedgerConstants.Constant.*;
import static state_engine.transaction.State.*;
import static state_engine.utils.PartitionHelper.getPartition_interval;
import static xerial.jnuma.Numa.setLocalAlloc;
public class SLInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(SLInitializer.class);
    public SLInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        configure_store(scale_factor, theta, tthread, NUM_ACCOUNTS);
    }
    @Override
    public void loadDB(int thread_id, int NUM_TASK) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUM_TASK - 1) {//last executor need to handle left-over
            right_bound = NUM_ACCOUNTS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0);
        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }
    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUM_TASK) {
        int partition_interval = getPartition_interval();
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUM_TASK - 1) {//last executor need to handle left-over
            right_bound = NUM_ACCOUNTS;
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        for (int key = left_bound; key < right_bound; key++) {
            int pid = get_pid(partition_interval, key);
            String _key = GenerateKey(ACCOUNT_ID_PREFIX, key);
            insertAccountRecord(_key, 0, pid, spinlock);
            _key = GenerateKey(BOOK_ENTRY_ID_PREFIX, key);
            insertAssetRecord(_key, 0, pid, spinlock);
        }
        LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
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
            db.InsertRecord("bookEntries", new TableRecord(AssetRecord(key, value)));
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
        setLocalAlloc();
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
        int elements_per_socket;
        setLocalAlloc();
        if (OsUtils.isMac())
            elements_per_socket = elements;
        else
            elements_per_socket = elements / 4;
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
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + String.valueOf(enable_states_partition))
                + OsUtils.OS_wrapper("NUM_EVENTS=" + String.valueOf(NUM_EVENTS))
                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + String.valueOf(config.getDouble("ratio_of_multi_partition", 1)))
                + OsUtils.OS_wrapper("number_partitions=" + String.valueOf(number_partitions));
        if (Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file))))
            return false;
        return true;
    }
    @Override
    public void store(String file_name) throws IOException {
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + String.valueOf(enable_states_partition))
                + OsUtils.OS_wrapper("NUM_EVENTS=" + String.valueOf(NUM_EVENTS))
                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + String.valueOf(config.getDouble("ratio_of_multi_partition", 1)))
                + OsUtils.OS_wrapper("number_partitions=" + String.valueOf(number_partitions));
        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist
        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(
                event_path
                        + OsUtils.OS_wrapper(file_name)
        )));
        for (Object event : db.getEventManager().input_events) {
            StringBuilder sb = new StringBuilder();
            if (event instanceof DepositEvent) {
                sb.append(((DepositEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getPid());//1
                sb.append(split_exp);
                sb.append(Arrays.toString(((DepositEvent) event).getBid_array()));//2
                sb.append(split_exp);
                sb.append(((DepositEvent) event).num_p());////3 num of p
                sb.append(split_exp);
                sb.append("DepositEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getAccountId());//5
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getBookEntryId());//6
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getAccountTransfer());//7
                sb.append(split_exp);
                sb.append(((DepositEvent) event).getBookEntryTransfer());//8
            } else if (event instanceof TransactionEvent) {
                sb.append(((TransactionEvent) event).getBid());//0 -- bid
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getPid());//1 -- pid.
                sb.append(split_exp);
                sb.append(Arrays.toString(((TransactionEvent) event).getBid_array()));//2 -- bid array
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).num_p());//3 num of p
                sb.append(split_exp);
                sb.append("TransactionEvent");//input_event types.
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getSourceAccountId());//5
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getSourceBookEntryId());//6
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getTargetAccountId());//7
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getTargetBookEntryId());//8
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getAccountTransfer());//9
                sb.append(split_exp);
                sb.append(((TransactionEvent) event).getBookEntryTransfer());//10
            }
            w.write(sb.toString() + "\n");
        }
        w.close();
    }
    private Object randomTransactionEvent(int partition_id, long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {
        final long accountsTransfer = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long transfer = rnd.nextLong(MAX_BOOK_TRANSFER);
        while (!Thread.currentThread().isInterrupted()) {
            int _pid = partition_id;
            final int sourceAcct;
            if (enable_states_partition)
                sourceAcct = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;
            else
                sourceAcct = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
            if (number_of_partitions > 1) {//multi-partition
                _pid++;
                if (_pid == tthread)
                    _pid = 0;
            }
            final int targetAcct;
            if (enable_states_partition)
                targetAcct = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;
            else
                targetAcct = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
            if (number_of_partitions > 1) {//multi-partition
                _pid++;
                if (_pid == tthread)
                    _pid = 0;
            }
            final int sourceBook;
            if (enable_states_partition)
                sourceBook = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;
            else
                sourceBook = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
            if (number_of_partitions > 1) {//multi-partition
                _pid++;
                if (_pid == tthread)
                    _pid = 0;
            }
            final int targetBook;
            if (enable_states_partition)
                targetBook = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;
            else
                targetBook = shared_store.next();//rnd.nextInt(asset_range) + partition_offset;
            if (sourceAcct == targetAcct || sourceBook == targetBook) {
                continue;
            }
            return new TransactionEvent(
                    bid,
                    partition_id,
                    bid_array,
                    number_of_partitions,
                    ACCOUNT_ID_PREFIX + sourceAcct,
                    BOOK_ENTRY_ID_PREFIX + sourceBook,
                    ACCOUNT_ID_PREFIX + targetAcct,
                    BOOK_ENTRY_ID_PREFIX + targetBook,
                    accountsTransfer,
                    transfer,
                    MIN_BALANCE);
        }
        return null;
    }
    /**
     * @param partition_id
     * @param bid_array
     * @param number_of_partitions
     * @param bid
     * @param rnd
     * @return
     */
    private Object randomDepositEvent(int partition_id,
                                      long[] bid_array, int number_of_partitions, long bid, SplittableRandom rnd) {
        int _pid = partition_id;
        final int account; //key
        if (enable_states_partition)
            account = partioned_store[_pid].next();//rnd.nextInt(account_range) + partition_offset;
        else
            account = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
        if (number_of_partitions > 1) {//multi-partition
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
        final int book;
        if (enable_states_partition)
            book = partioned_store[_pid].next();//rnd.nextInt(asset_range) + partition_offset;
        else
            book = shared_store.next();//rnd.nextInt(account_range) + partition_offset;
        //value_list
        final long accountsDeposit = rnd.nextLong(MAX_ACCOUNT_TRANSFER);
        final long deposit = rnd.nextLong(MAX_BOOK_TRANSFER);
        return new DepositEvent(
                bid,
                partition_id,
                bid_array,
                number_of_partitions,
                ACCOUNT_ID_PREFIX + account,
                BOOK_ENTRY_ID_PREFIX + book,
                accountsDeposit,
                deposit);
    }
    @Override
    public Object create_new_event(int num_p, int bid) {
        int flag = next_decision2();
        if (flag == 0) {
            if (num_p != 1)
                return randomDepositEvent(p, p_bid.clone(), 2, bid, rnd);
            return randomDepositEvent(p, p_bid.clone(), 1, bid, rnd);
        } else if (flag == 1) {
            if (num_p != 1)
                return randomTransactionEvent(p, p_bid.clone(), 4, bid, rnd);
            return randomTransactionEvent(p, p_bid.clone(), 1, bid, rnd);
        }
        return null;
    }
    public void creates_Table(Configuration config) {
        RecordSchema s = AccountsScheme();
        db.createTable(s, "accounts");
        RecordSchema b = BookEntryScheme();
        db.createTable(b, "bookEntries");
        try {
            prepare_input_events("SL_Events", false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
