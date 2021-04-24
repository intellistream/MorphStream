package common.param.ob;
import common.bolts.transactional.ob.BidingResult;
import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;
import java.util.SplittableRandom;

import static common.constants.OnlineBidingSystemConstants.Constant.*;
public class BuyingEvent extends TxnEvent {
    //place-rangeMap.
    public SchemaRecordRef[] record_refs;
    //updated state...to be written.
    public long newQty;
    public BidingResult biding_result;
    //expected state.
    //long Item_value=0;
    //long asset_value=0;
    private int[] itemId;
    private long[] bid_price;
    private long[] bid_qty;
    /**
     * Creates a new BuyingEvent.
     */
    public BuyingEvent(int[] itemId, SplittableRandom rnd, int partition_id, long[] bid_array, long bid, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.itemId = itemId;
        record_refs = new SchemaRecordRef[NUM_ACCESSES_PER_BUY];
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        bid_price = new long[NUM_ACCESSES_PER_BUY];
        bid_qty = new long[NUM_ACCESSES_PER_BUY];
        setValues(rnd);
    }
    /**
     * Loading a BuyingEvent.
     */
    public BuyingEvent(int bid, String bid_array, int pid, int num_of_partition,
                       String key_array, String price_array, String qty_array) {
        super(bid, pid, bid_array, num_of_partition);
        record_refs = new SchemaRecordRef[NUM_ACCESSES_PER_BUY];
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; i++) {
            record_refs[i] = new SchemaRecordRef();
        }
        bid_price = new long[NUM_ACCESSES_PER_BUY];
        bid_qty = new long[NUM_ACCESSES_PER_BUY];
        String[] key_arrays = key_array.substring(1, key_array.length() - 1).split(",");
        this.itemId = new int[key_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.itemId[i] = Integer.parseInt(key_arrays[i].trim());
        }
        String[] price_arrays = price_array.substring(1, price_array.length() - 1).split(",");
        this.bid_price = new long[price_arrays.length];
        for (int i = 0; i < key_arrays.length; i++) {
            this.bid_price[i] = Long.parseLong(price_arrays[i].trim());
        }
        String[] qty_arrays = qty_array.substring(1, qty_array.length() - 1).split(",");
        this.bid_qty = new long[qty_arrays.length];
        for (int i = 0; i < qty_arrays.length; i++) {
            this.bid_qty[i] = Long.parseLong(qty_arrays[i].trim());
        }
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public long getBidPrice(int access_id) {
        return bid_price[access_id];
    }
    public long getBidQty(int access_id) {
        return bid_qty[access_id];
    }
    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------
    @Override
    public String toString() {
        return "BuyingEvent {"
                + "itemId=" + Arrays.toString(itemId)
                + ", bid_price=" + Arrays.toString(bid_price)
                + ", bid_qty=" + Arrays.toString(bid_qty)
                + '}';
    }
    public int[] getItemId() {
        return itemId;
    }
    public long[] getBidPrice() {
        return bid_price;
    }
    public long[] getBidQty() {
        return bid_qty;
    }
    private void set_values(int access_id, SplittableRandom rnd) {
        bid_price[access_id] = rnd.nextLong(MAX_Price);
        bid_qty[access_id] = rnd.nextLong(MAX_BUY_Transfer);
    }
    public void setValues(SplittableRandom rnd) {
        for (int access_id = 0; access_id < NUM_ACCESSES_PER_BUY; ++access_id) {
            set_values(access_id, rnd);
        }
    }
}
