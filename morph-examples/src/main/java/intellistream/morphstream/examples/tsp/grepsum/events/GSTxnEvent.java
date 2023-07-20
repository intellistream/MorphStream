package intellistream.morphstream.examples.tsp.grepsum.events;

import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.datatype.IntDataBox;
import intellistream.morphstream.engine.txn.storage.datatype.StringDataBox;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static intellistream.morphstream.common.constants.GrepSumConstants.Constant.VALUE_LEN;
import static intellistream.morphstream.engine.txn.profiler.Metrics.NUM_ACCESSES;

/**
 * Support Multi workset since 1 SEP 2018.
 */
public class GSTxnEvent extends TxnEvent {
    private final SchemaRecordRef[] record_refs;//this is essentially the place-holder..
    private final int[] keys;
    private final boolean abortFlag; // whether to abort the event
    public int sum;
    public int TOTAL_NUM_ACCESS;
    public int Txn_Length;
    public int[] result = new int[TOTAL_NUM_ACCESS]; // TODO: NUM_ACCESSES will be never used.
    private List<DataBox>[] value;//Note, it should be arraylist instead of linkedlist as there's no addOperation/remove later.
    //    public double[] useful_ratio = new double[1];

    /**
     * creating a new MicroEvent.
     *
     * @param keys
     * @param flag
     * @param numAccess
     */
    public GSTxnEvent(int[] keys, boolean flag, int numAccess, long bid
            , int partition_id, long[] bid_array, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.abortFlag = flag;
        this.keys = keys;
        record_refs = new SchemaRecordRef[numAccess];
        for (int i = 0; i < numAccess; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        setValues(keys);
    }

    /**
     * Loading a DepositEvent.
     *
     * @param flag,            read_write flag
     * @param bid
     * @param pid
     * @param bid_array
     * @param num_of_partition
     * @param key_array
     */
    public GSTxnEvent(int bid, int pid, String bid_array, int num_of_partition,
                      String key_array, boolean flag) {
        super(bid, pid, bid_array, num_of_partition);
        record_refs = new SchemaRecordRef[NUM_ACCESSES];
        for (int i = 0; i < NUM_ACCESSES; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.abortFlag = flag;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
        setValues(keys);
    }

    public GSTxnEvent(int bid, int pid, String bid_array, String partition_index, int num_of_partition,
                      String key_array, int TOTAL_NUM_ACCESS, boolean flag) {
        super(bid, pid, bid_array, partition_index, num_of_partition);
        this.TOTAL_NUM_ACCESS = TOTAL_NUM_ACCESS;
        result = new int[TOTAL_NUM_ACCESS];
        record_refs = new SchemaRecordRef[TOTAL_NUM_ACCESS];
        for (int i = 0; i < TOTAL_NUM_ACCESS; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.abortFlag = flag;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
        setValues(keys);
    }


    public GSTxnEvent(int bid, int pid, String bid_array, String partition_index, int num_of_partition,
                      String key_array, int TOTAL_NUM_ACCESS, int Transaction_Length, boolean abortFlag) {
        super(bid, pid, bid_array, partition_index, num_of_partition);
        this.TOTAL_NUM_ACCESS = TOTAL_NUM_ACCESS;
        Txn_Length = Transaction_Length;
        result = new int[TOTAL_NUM_ACCESS];
        record_refs = new SchemaRecordRef[TOTAL_NUM_ACCESS];
        for (int i = 0; i < TOTAL_NUM_ACCESS; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.abortFlag = abortFlag;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
        setValues(keys);
    }

    private static String rightpad(String text, int length) {
        return StringUtils.rightPad(text, length); // Returns "****foobar"
//        return String.format("%-" + length + "." + length + "s", text);
    }

    public static String GenerateValue(int key) {
        return rightpad(String.valueOf(key), VALUE_LEN);
    }

    public int[] getKeys() {
        return keys;
    }

    public List<DataBox>[] getValues() {
        return value;
    }

    public void setValues(int[] keys) {
        value = new ArrayList[TOTAL_NUM_ACCESS];//Note, it should be arraylist instead of linkedlist as there's no addOperation/remove later.
        for (int access_id = 0; access_id < TOTAL_NUM_ACCESS; ++access_id) {
            set_values(access_id, keys[access_id]);
        }
    }

    public SchemaRecordRef[] getRecord_refs() {
        return record_refs;
    }

    public boolean ABORT_EVENT() {
        return abortFlag;
    }

    private void set_values(int access_id, int key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));//key  4 bytes
        values.add(new StringDataBox(GenerateValue(key), VALUE_LEN));//value_list   32 bytes..
        value[access_id] = values;
    }

    public GSTxnEvent cloneEvent() {
        return new GSTxnEvent((int) bid, pid,
                Arrays.toString(bid_array), Arrays.toString(partition_indexs),
                number_of_partitions, Arrays.toString(keys), TOTAL_NUM_ACCESS, Txn_Length, abortFlag);
    }
}