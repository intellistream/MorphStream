package common.param.mb;

import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;

import java.util.Arrays;

public class NonMicroEvent extends TxnEvent {
    private final SchemaRecordRef[] record_refs;//this is essentially the place-holder..
    private final int[] keys;
    private final boolean isAbort;//true: abort.
    private final boolean isNon_Deterministic_StateAccess;//true: non-state access.
    public int sum;
    public int TOTAL_NUM_ACCESS;
    public int Txn_Length;
    public int[] result; // TODO: NUM_ACCESSES will be never used.

    public NonMicroEvent(long bid, int pid, String bid_array, String partition_index, int num_of_partition, String key_array, int TOTAL_NUM_ACCESS, int Transaction_Length, boolean isAbort, boolean isNon_Deterministic_StateAccess) {
        super(bid, pid, bid_array, partition_index, num_of_partition);
        this.TOTAL_NUM_ACCESS = TOTAL_NUM_ACCESS;
        this.Txn_Length = Transaction_Length;
        result = new int[TOTAL_NUM_ACCESS];
        record_refs = new SchemaRecordRef[TOTAL_NUM_ACCESS];
        for (int i = 0; i < TOTAL_NUM_ACCESS; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.isAbort = isAbort;
        this.isNon_Deterministic_StateAccess = isNon_Deterministic_StateAccess;
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.keys = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.keys[i] = Integer.parseInt(key_arrays[i].trim());
        }
    }

    public boolean isAbort() {
        return isAbort;
    }

    public boolean isNon_Deterministic_StateAccess() {
        return isNon_Deterministic_StateAccess;
    }

    public int[] getKeys() {
        return keys;
    }

    public SchemaRecordRef[] getRecord_refs() {
        return record_refs;
    }

    public NonMicroEvent cloneEvent() {
        return new NonMicroEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, Arrays.toString(keys), TOTAL_NUM_ACCESS, Txn_Length, isAbort, isNon_Deterministic_StateAccess);
    }
}