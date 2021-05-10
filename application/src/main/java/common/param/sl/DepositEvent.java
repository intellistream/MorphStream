package common.param.sl;
import common.param.TxnEvent;
import storage.SchemaRecordRef;
import storage.TableRecordRef;
import storage.datatype.DataBox;

import java.util.List;
public class DepositEvent extends TxnEvent {
    //updated state...to be written.
    public long newAccountValue;
    public long newAssetValue;
    //place-rangeMap.
    public SchemaRecordRef account_value = new SchemaRecordRef();
    public SchemaRecordRef asset_value = new SchemaRecordRef();
    //used in no-push.
    public TableRecordRef account_values = new TableRecordRef();
    public TableRecordRef asset_values = new TableRecordRef();
    private final String accountId; //32 bytes
    //expected state.
    //long Item_value=0;
    //long asset_value=0;
    private final String bookEntryId; //32 bytes
    private final long accountTransfer; //64 bytes
    private final long bookEntryTransfer;//64 bytes
    /**
     * Creates a new DepositEvent.
     */
    public DepositEvent(
            long bid, int partition_id, long[] bid_array, int number_of_partitions,
            String accountId,
            String bookEntryId,
            long accountTransfer,
            long bookEntryTransfer) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.accountId = accountId;
        this.bookEntryId = bookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
    }
    /**
     * Loading a DepositEvent.
     *
     * @param bid
     * @param pid
     * @param bid_array
     * @param num_of_partition
     * @param accountId
     * @param bookEntryId
     * @param accountTransfer
     * @param bookEntryTransfer
     */
    public DepositEvent(int bid, int pid, String bid_array, int num_of_partition,
                        String accountId,
                        String bookEntryId,
                        long accountTransfer,
                        long bookEntryTransfer) {
        super(bid, pid, bid_array, num_of_partition);
        this.accountId = accountId;
        this.bookEntryId = bookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
    }
    public String getAccountId() {
        return accountId;
    }
    public String getBookEntryId() {
        return bookEntryId;
    }
    public long getAccountTransfer() {
        return accountTransfer;
    }
    public long getBookEntryTransfer() {
        return bookEntryTransfer;
    }
    public List<DataBox> getUpdatedAcount_value() {
        return null;
    }
    public List<DataBox> getUpdatedAsset_value() {
        return null;
    }
    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------
    @Override
    public String toString() {
        return "DepositEvent {"
                + "accountId=" + accountId
                + ", bookEntryId=" + bookEntryId
                + ", accountTransfer=" + accountTransfer
                + ", bookEntryTransfer=" + bookEntryTransfer
                + '}';
    }
}
