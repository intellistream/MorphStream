package common.param.sl;

import common.param.TxnEvent;
import storage.SchemaRecordRef;
import storage.TableRecordRef;
import storage.datatype.DataBox;

import java.util.Arrays;
import java.util.List;

public class DepositEvent extends TxnEvent {
    private final String accountId; //32 bytes
    //expected state.
    //long Item_value=0;
    //long asset_value=0;
    private final String bookEntryId; //32 bytes
    private final long accountTransfer; //64 bytes
    private final long bookEntryTransfer;//64 bytes
    //updated state...to be written.
    public long newAccountValue;
    public long newAssetValue;
    //place-rangeMap.
    public SchemaRecordRef account_value = new SchemaRecordRef();
    public SchemaRecordRef asset_value = new SchemaRecordRef();
    //used in no-push.
    public TableRecordRef account_values = new TableRecordRef();
    public TableRecordRef asset_values = new TableRecordRef();

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
    public DepositEvent(int bid, int pid, String bid_array, String partition_index, int num_of_partition,
                        String accountId,
                        String bookEntryId,
                        long accountTransfer,
                        long bookEntryTransfer) {
        super(bid, pid, bid_array, partition_index, num_of_partition);
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
        return getBid() +
                "," + getAccountId() +
                "," + getBookEntryId() +
                "," + getAccountTransfer() +
                "," + getBookEntryTransfer() +
                "," + getTimestamp();
    }

    public DepositEvent cloneEvent() {
        return new DepositEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, accountId, bookEntryId, accountTransfer, bookEntryTransfer);
    }
}
