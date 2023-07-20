package intellistream.morphstream.examples.tsp.streamledger.events;

import intellistream.morphstream.examples.tsp.streamledger.op.TransactionResult;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.TableRecordRef;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;

import java.util.Arrays;
import java.util.List;

import static intellistream.morphstream.common.constants.StreamLedgerConstants.Constant.MIN_BALANCE;

public class TransactionTxnEvent extends TxnEvent {
    private final String sourceAccountId;
    private final String targetAccountId;
    private final String sourceBookEntryId;
    private final String targetBookEntryId;
    private final long accountTransfer;
    private final long bookEntryTransfer;
    private final long minAccountBalance;
    //embeded state.
    public volatile SchemaRecordRef src_account_value = new SchemaRecordRef();
    public volatile SchemaRecordRef dst_account_value = new SchemaRecordRef();
    public volatile SchemaRecordRef src_asset_value = new SchemaRecordRef();
    public volatile SchemaRecordRef dst_asset_value = new SchemaRecordRef();
    public volatile TableRecordRef src_account_values = new TableRecordRef();
    public volatile TableRecordRef dst_account_values = new TableRecordRef();
    public volatile TableRecordRef src_asset_values = new TableRecordRef();
    public volatile TableRecordRef dst_asset_values = new TableRecordRef();
    public TransactionResult transaction_result;

    /**
     * Creates a new TransactionEvent for the given accounts and book entries.
     */
    public TransactionTxnEvent(int bid, int partition_id, String bid_array, String partition_index,
                               int num_of_partition,
                               String sourceAccountId,
                               String sourceBookEntryId,
                               String targetAccountId,
                               String targetBookEntryId,
                               long accountTransfer,
                               long bookEntryTransfer) {
        super(bid, partition_id, bid_array, partition_index, num_of_partition);
        this.sourceAccountId = sourceAccountId;
        this.targetAccountId = targetAccountId;
        this.sourceBookEntryId = sourceBookEntryId;
        this.targetBookEntryId = targetBookEntryId;
        this.accountTransfer = accountTransfer;
        this.bookEntryTransfer = bookEntryTransfer;
        this.minAccountBalance = MIN_BALANCE;
    }

    public String getSourceAccountId() {
        return sourceAccountId;
    }

    public String getTargetAccountId() {
        return targetAccountId;
    }

    public String getSourceBookEntryId() {
        return sourceBookEntryId;
    }

    public String getTargetBookEntryId() {
        return targetBookEntryId;
    }

    public long getAccountTransfer() {
        return accountTransfer;
    }

    public long getBookEntryTransfer() {
        return bookEntryTransfer;
    }

    public long getMinAccountBalance() {
        return minAccountBalance;
    }

    public List<DataBox> getUpdatedSourceBalance() {
        return null;
    }

    public List<DataBox> getUpdatedTargetBalance() {
        return null;
    }

    public List<DataBox> getUpdatedSourceAsset_value() {
        return null;
    }

    public List<DataBox> getUpdatedTargetAsset_value() {
        return null;
    }

    public TransactionTxnEvent cloneEvent() {
        return new TransactionTxnEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, sourceAccountId, sourceBookEntryId, targetAccountId, targetBookEntryId, accountTransfer, bookEntryTransfer);
    }

    // ------------------------------------------------------------------------
    //  miscellaneous
    // ------------------------------------------------------------------------
    @Override
    public String toString() {
        return getBid() +
                "," + getSourceAccountId() +
                "," + getSourceBookEntryId() +
                "," + getTargetAccountId() +
                "," + getTargetBookEntryId() +
                "," + getAccountTransfer() +
                "," + getBookEntryTransfer() +
                "," + getTimestamp();
    }
}
