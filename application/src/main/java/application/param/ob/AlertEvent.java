package application.param.ob;

import application.param.TxnEvent;
import state_engine.storage.SchemaRecordRef;

import java.util.Arrays;
import java.util.SplittableRandom;

import static application.constants.OnlineBidingSystemConstants.Constant.MAX_Price;

public class AlertEvent extends TxnEvent {


    private final int num_access;
    public boolean alert_result;
    //place-rangeMap.
    public SchemaRecordRef[] record_refs;
    private int[] itemId;//keys.
    private long[] ask_price;//new ask price
    /**
     * Creates a new AlertEvent.
     */
    public AlertEvent(
            int num_access, int[] itemId,
            SplittableRandom rnd,
            int partition_id, long[] bid_array, long bid, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.num_access = num_access;
        record_refs = new SchemaRecordRef[num_access];
        for (int i = 0; i < num_access; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        this.itemId = itemId;

        setValues(num_access, rnd);
    }

    public AlertEvent(int bid, String bid_array, int partition_id, int number_of_partitions,
                      int num_access, String key_array, String alert_array) {
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

        String[] top_arrays = alert_array.substring(1, alert_array.length() - 1).split(",");
        this.ask_price = new long[top_arrays.length];

        for (int i = 0; i < top_arrays.length; i++) {
            this.ask_price[i] = Long.parseLong(top_arrays[i].trim());
        }
    }

    public int getNum_access() {
        return num_access;
    }

    private void setValues(int num_access, SplittableRandom rnd) {

        ask_price = new long[num_access];
        for (int access_id = 0; access_id < num_access; ++access_id) {
            set_values(access_id, rnd);
        }
    }

    private void set_values(int access_id, SplittableRandom rnd) {
        ask_price[access_id] = rnd.nextLong(MAX_Price);
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

    public long[] getAsk_price() {
        return ask_price;
    }

    @Override
    public String toString() {
        return "AlertEvent {"
                + "itemId=" + Arrays.toString(itemId)
                + ", ask_price=" + Arrays.toString(ask_price)
                + '}';
    }
}
