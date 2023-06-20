package common.param.lb;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;


/**
 * Support Multi workset since 1 SEP 2018.
 */
public class LBEvent extends TxnEvent {
    private final String connID;
    public String serverID; //server to connect after load balancing
    private final SchemaRecordRef[] record_refs;//this is essentially the place-holder..
    private final int[] keys;
    private final boolean isNewConn;//true: state access; false: no state access
    private final int newConnBID;
    private double[] newConnBIDArray;
    public int sum;
    public int TOTAL_NUM_ACCESS;
    public int Txn_Length;

    public LBEvent(int bid, int pid, String bid_array, String partition_index, int num_of_partition,
                      String key_array, int TOTAL_NUM_ACCESS, int Transaction_Length, boolean isNewConn, String connID, int newConnBID) {
        super(bid, pid, bid_array, partition_index, num_of_partition);
        this.TOTAL_NUM_ACCESS = TOTAL_NUM_ACCESS;
        Txn_Length = Transaction_Length;
        record_refs = new SchemaRecordRef[TOTAL_NUM_ACCESS];
        for (int i = 0; i < TOTAL_NUM_ACCESS; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.isNewConn = isNewConn;
        this.connID = connID;
        this.newConnBID = newConnBID;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
    }

    public int[] getKeys() {
        return keys;
    }
    public String getConnID() {
        return connID;
    }
    public int getNewConnBID() {
        return newConnBID;
    }

    public SchemaRecordRef[] getRecord_refs() {
        return record_refs;
    }

    public boolean isNewConn() {
        return isNewConn;
    }

    public LBEvent cloneEvent() {
        return new LBEvent((int) bid, pid,
                Arrays.toString(bid_array), Arrays.toString(partition_indexs),
                number_of_partitions, Arrays.toString(keys), TOTAL_NUM_ACCESS, Txn_Length, isNewConn, connID, newConnBID);
    }

}