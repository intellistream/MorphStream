package common.param.mb;

import common.param.TxnEvent;
import common.param.sl.TransactionEvent;
import org.apache.commons.lang.StringUtils;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import storage.datatype.IntDataBox;
import storage.datatype.StringDataBox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static common.constants.GrepSumConstants.Constant.VALUE_LEN;
import static profiler.Metrics.NUM_ACCESSES;

/**
 * Support Multi workset since 1 SEP 2018.
 */
public class MicroEvent extends TxnEvent {
    private final SchemaRecordRef[] record_refs;//this is essentially the place-holder..
    private final int[] keys;
    private final boolean flag;//true: read, false: write.
    public int sum;
    public int NUM_ACCESS;
    public int[] result = new int[NUM_ACCESSES]; // TODO: NUM_ACCESSES will be never used.
    private List<DataBox>[] value;//Note, it should be arraylist instead of linkedlist as there's no addOperation/remove later.
    //    public double[] useful_ratio = new double[1];

    /**
     * creating a new MicroEvent.
     *
     * @param keys
     * @param flag
     * @param numAccess
     */
    public MicroEvent(int[] keys, boolean flag, int numAccess, long bid
            , int partition_id, long[] bid_array, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.flag = flag;
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
    public MicroEvent(int bid, int pid, String bid_array, int num_of_partition,
                      String key_array, boolean flag) {
        super(bid, pid, bid_array, num_of_partition);
        record_refs = new SchemaRecordRef[NUM_ACCESSES];
        for (int i = 0; i < NUM_ACCESSES; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.flag = flag;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
        setValues(keys);
    }

    public MicroEvent(int bid, int pid, String bid_array, String partition_index, int num_of_partition,
                      String key_array, int NUM_ACCESS, boolean flag) {
        super(bid, pid, bid_array, partition_index, num_of_partition);
        this.NUM_ACCESS = NUM_ACCESS;
        result = new int[NUM_ACCESS];
        record_refs = new SchemaRecordRef[NUM_ACCESS];
        for (int i = 0; i < NUM_ACCESS; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.flag = flag;
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
        value = new ArrayList[NUM_ACCESS];//Note, it should be arraylist instead of linkedlist as there's no addOperation/remove later.
        for (int access_id = 0; access_id < NUM_ACCESS; ++access_id) {
            set_values(access_id, keys[access_id]);
        }
    }

    public SchemaRecordRef[] getRecord_refs() {
        return record_refs;
    }

    public boolean READ_EVENT() {
        return flag;
    }

    private void set_values(int access_id, int key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));//key  4 bytes
        values.add(new StringDataBox(GenerateValue(key), VALUE_LEN));//value_list   32 bytes..
        value[access_id] = values;
    }

    public MicroEvent cloneEvent() {
        return new MicroEvent((int) bid, pid,
                Arrays.toString(bid_array), Arrays.toString(partition_indexs),
                number_of_partitions, Arrays.toString(keys), NUM_ACCESS, flag);
    }
}