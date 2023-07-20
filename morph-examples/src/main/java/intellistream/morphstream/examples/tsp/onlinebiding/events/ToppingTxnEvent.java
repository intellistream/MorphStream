package intellistream.morphstream.examples.tsp.onlinebiding.events;

import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;

import java.util.Arrays;
import java.util.SplittableRandom;

import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Constant.MAX_TOP_UP;

public class ToppingTxnEvent extends TxnEvent {
    private final int num_access;
    private final int[] itemId;
    //place-rangeMap.
    public SchemaRecordRef[] record_refs;
    public boolean topping_result;
    private long[] itemTopUp;

    /**
     * Creates a new ToppingEvent.
     */
    public ToppingTxnEvent(
            int num_access, int[] itemId,
            SplittableRandom rnd,
            int partition_id, String bid_array, long bid, int number_of_partitions, String partition_index) {
        super(bid, partition_id, bid_array, partition_index, number_of_partitions);
        record_refs = new SchemaRecordRef[num_access];
        this.num_access = num_access;
        for (int i = 0; i < num_access; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.itemId = itemId;
        setValues(num_access, rnd);
    }

    /**
     * @param bid
     * @param bid_array
     * @param partition_id
     * @param number_of_partitions
     * @param num_access
     * @param key_array
     * @param top_array
     */
    public ToppingTxnEvent(int bid, String bid_array, int partition_id, int number_of_partitions,
                           int num_access, String key_array, String top_array) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.num_access = num_access;
        record_refs = new SchemaRecordRef[num_access];
        for (int i = 0; i < num_access; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.itemId = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.itemId[i] = Integer.parseInt(key_arrays[i].trim());
        }
        String[] top_arrays = top_array.substring(1, top_array.length() - 1).split(",");
        this.itemTopUp = new long[top_arrays.length];
        for (int i = 0; i < top_arrays.length; i++) {
            this.itemTopUp[i] = Long.parseLong(top_arrays[i].trim());
        }
    }

    public int getNum_access() {
        return num_access;
    }

    private void setValues(int num_access, SplittableRandom rnd) {
        itemTopUp = new long[num_access];
        for (int access_id = 0; access_id < num_access; ++access_id) {
            set_values(access_id, rnd);
        }
    }

    private void set_values(int access_id, SplittableRandom rnd) {
        itemTopUp[access_id] = rnd.nextLong(MAX_TOP_UP);
    }

    public int[] getItemId() {
        return itemId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------
    public long[] getItemTopUp() {
        return itemTopUp;
    }

    @Override
    public String toString() {
        return "ToppingEvent {"
                + "itemId=" + Arrays.toString(itemId)
                + ", itemTopUp=" + Arrays.toString(itemTopUp)
                + '}';
    }

    @Override
    public ToppingTxnEvent cloneEvent() {
        return new ToppingTxnEvent((int) bid, Arrays.toString(bid_array), pid, number_of_partitions, num_access, Arrays.toString(itemId), Arrays.toString(itemTopUp));
    }
}
