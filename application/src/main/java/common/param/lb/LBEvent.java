package common.param.lb;

import common.param.TxnEvent;
import common.param.mb.MicroEvent;
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
public class LBEvent extends TxnEvent {
    private final String connID;
//    public boolean isNewConn; //TODO: Improve this, newConn is determined in bolts
    public String serverID;
    private final SchemaRecordRef[] record_refs;//this is essentially the place-holder..
    private final int[] keys;
    private final boolean isNewConn;//true: read, false: write.
    public int sum;
    public int TOTAL_NUM_ACCESS;
    public int Txn_Length;
    public int[] result = new int[TOTAL_NUM_ACCESS]; // TODO: NUM_ACCESSES will be never used.
    private List<DataBox>[] value;//Note, it should be arraylist instead of linkedlist as there's no addOperation/remove later.
    //    public double[] useful_ratio = new double[1];

    public LBEvent(int bid, int pid, String bid_array, String partition_index, int num_of_partition,
                      String key_array, int TOTAL_NUM_ACCESS, int Transaction_Length, boolean isNewConn, String connID) {
        super(bid, pid, bid_array, partition_index, num_of_partition);
        this.TOTAL_NUM_ACCESS = TOTAL_NUM_ACCESS;
        Txn_Length = Transaction_Length;
        result = new int[TOTAL_NUM_ACCESS];
        record_refs = new SchemaRecordRef[TOTAL_NUM_ACCESS];
        for (int i = 0; i < TOTAL_NUM_ACCESS; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.isNewConn = isNewConn;
        this.connID = connID;
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

    public boolean isNewConn() {
        return isNewConn;
    }

    private void set_values(int access_id, int key) {
        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(key));//key  4 bytes
        values.add(new StringDataBox(GenerateValue(key), VALUE_LEN));//value_list   32 bytes..
        value[access_id] = values;
    }

    public LBEvent cloneEvent() {
        return new LBEvent((int) bid, pid,
                Arrays.toString(bid_array), Arrays.toString(partition_indexs),
                number_of_partitions, Arrays.toString(keys), TOTAL_NUM_ACCESS, Txn_Length, isNewConn, connID);
    }

//    private final double myBid;
//    private final int myPid;
//    private final String my_bid_array;
//    private final String my_partition_index;
//    private final int my_number_of_partitions;
//
//    public LBEvent(double bid, int pid, String bid_array, String partition_index, int number_of_partitions,
//                   String connID, String srcAddr, String srcPort) {
//        super(bid, pid, bid_array, partition_index, number_of_partitions);
//        this.myBid = bid;
//        this.myPid = pid;
//        this.my_bid_array = bid_array;
//        this.my_partition_index = partition_index;
//        this.my_number_of_partitions = number_of_partitions;
//        this.connID = connID;
//        this.srcAddr = srcAddr;
//        this.srcPort = srcPort;
//    }
//
//    public String getConnID() {
//        return connID;
//    }
//    public String getSrcAddr() {
//        return srcAddr;
//    }
//    public String getSrcPort() {
//        return srcPort;
//    }
//    public double getMyBid() {
//        return myBid;
//    }
//    public int getMyPid() {
//        return myPid;
//    }
//    public String getMyBidArray() {
//        return my_bid_array;
//    }
//    public String getMyPartitionIndex() {
//        return my_partition_index;
//    }
//    public int getMyNumberOfPartitions() {
//        return my_number_of_partitions;
//    }
//
//
//    public LBEvent cloneEvent() {
//        return new LBEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions,
//                connID, srcAddr, srcPort);
//    }
}