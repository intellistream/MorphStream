package intellistream.morphstream.engine.txn.transaction.impl.ordered;

import intellistream.morphstream.engine.txn.content.Content;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.StorageManager;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.transaction.context.TxnAccess;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.impl.TxnManagerDedicatedLocked;
import intellistream.morphstream.transNFV.common.VNFRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static intellistream.morphstream.common.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.*;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.kMaxAccessNum;
import static intellistream.morphstream.util.PartitionHelper.key_to_partition;

/**
 * mimic of S-Store
 */
public class TxnManagerSStore extends TxnManagerDedicatedLocked {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerSStore.class);
    public final PartitionedOrderLock orderLock;
    public final OrderLock shared_orderLock;

    public TxnManagerSStore(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);
        this.shared_orderLock = OrderLock.getInstance();
        this.orderLock = PartitionedOrderLock.getInstance();
    }

    public OrderLock getOrderLock() {
        return shared_orderLock;
    }

    public PartitionedOrderLock.LOCK getOrderLock(int p_id) {
        return orderLock.get(p_id);
    }

    @Override
    public boolean submitStateAccess(VNFRequest vnfRequest, TxnContext txnContext) throws DatabaseException {
        return false;
    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap)
            throws DatabaseException {
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record, (int) this.thread_count_);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            while (!tb_record.content_.TryWriteLock()) {
                txn_context.is_retry_ = true;//retry, no abort..
            }
            record.is_visible_ = true;
            TxnAccess.Access access = access_list_.NewAccess();
            access.access_type_ = INSERT_ONLY;
            access.access_record_ = tb_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = 0;
            return true;
        } else {
            return true;
        }
    }

    @Override
    protected boolean lock_aheadCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
//        record_ref.setRecord(t_record.record_);//Note that, locking scheme allows directly modifying on original table d_record.
//        if (enable_log) LOG.info("LOCK FOR:" + t_record.record_.getValues().get(0)+" pid:"+txn_context.pid);
        t_record.content_.LockPartitions();//it should always success.
        return true;
    }

    @Override
    public boolean lock_all(SpinLock[] spinLocks) throws DatabaseException {
        for (SpinLock spinLock : spinLocks) {
            spinLock.lock();
        }
        return true;
    }

    @Override
    public boolean unlock_all(SpinLock[] spinLocks) throws DatabaseException {
        for (SpinLock spinLock : spinLocks) {
            spinLock.unlock();
        }
        return true;
    }

    @Override
    public boolean SelectKeyRecord_noLockCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        record_ref.setRecord(t_record.record_);//Note that, locking scheme allows directly modifying on original table d_record.
        if (accessType == READ_ONLY) {
            TxnAccess.Access access = access_list_.NewAccess();
            access.access_type_ = READ_ONLY;
            access.access_record_ = t_record;
//            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            return true;
        } else if (accessType == READ_WRITE) {
            //if (enable_log) LOG.info(txn_context.thisTaskId + " success to get orderLock" + DateTime.now());
//            final SchemaRecord local_record = new SchemaRecord(t_record.record_);//copy from t_record to local_record.
            TxnAccess.Access access = access_list_.NewAccess();
            access.access_type_ = READ_WRITE;
            access.access_record_ = t_record;
//            access.local_record_ = local_record;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            return true;
        } else if (accessType == DELETE_ONLY) {
            //if (enable_log) LOG.info(t_record.toString() + "is locked by deleter");
            t_record.record_.is_visible_ = false;
            TxnAccess.Access access = access_list_.NewAccess();
            access.access_type_ = DELETE_ONLY;
            access.access_record_ = t_record;
//            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            return true;
        } else if (accessType == NON_READ_WRITE_COND_READN) {
            TxnAccess.Access access = access_list_.NewAccess();
            access.access_type_ = NON_READ_WRITE_COND_READN;
            access.access_record_ = t_record;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            SchemaRecord srcRecord = t_record.record_;
            long srcValue = srcRecord.getValues().get(1).getLong();
            // Read the corresponding value
            TableRecord deterministicSchemaRecord = this.storageManager_.getTable(table_name).SelectKeyRecord(String.valueOf(srcValue));
            record_ref.setRecord(deterministicSchemaRecord.record_);
            return true;
        } else {
            assert (false);
            return false;
        }
    }

    @Override
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean CommitTransaction(TxnContext txn_context) {
        long[] partition_bid = txn_context.partition_bid;
        if (partition_bid != null) {
            long commit_ts;
            for (int i = 0; i < access_list_.access_count_; ++i) {
                TxnAccess.Access access_ptr = access_list_.GetAccess(i);
                Content content_ref = access_ptr.access_record_.content_;
                if (access_ptr.access_type_ == READ_WRITE || access_ptr.access_type_ == NON_READ_WRITE_COND_READN) {
                    int p = key_to_partition(access_ptr.access_record_.record_.GetPrimaryKey());
                    commit_ts = partition_bid[p];
                    assert (commit_ts >= access_ptr.timestamp_);
                    content_ref.SetTimestamp(commit_ts);
                } else if (access_ptr.access_type_ == INSERT_ONLY) {
                    int p = key_to_partition(access_ptr.access_record_.record_.GetPrimaryKey());
                    commit_ts = partition_bid[p];
                    assert (commit_ts >= access_ptr.timestamp_);
                    content_ref.SetTimestamp(commit_ts);
                } else if (access_ptr.access_type_ == DELETE_ONLY) {
                    int p = (int) Math.floor(Integer.parseInt(access_ptr.access_record_.record_.GetPrimaryKey()) / (int) (Math.ceil(NUM_ACCOUNTS / (double) thread_count_)));
                    commit_ts = partition_bid[p];
                    assert (commit_ts >= access_ptr.timestamp_);
                    content_ref.SetTimestamp(commit_ts);
                }
            }
        } else {
            long commit_ts = txn_context.getBID();//This makes the execution appears to execute at one atomic time unit. //GenerateMonotoneTimestamp(curr_epoch, GlobalTimestamp.GetMonotoneTimestamp());
            for (int i = 0; i < access_list_.access_count_; ++i) {
                TxnAccess.Access access_ptr = access_list_.GetAccess(i);
                Content content_ref = access_ptr.access_record_.content_;
                if (access_ptr.access_type_ == READ_WRITE || access_ptr.access_type_ == NON_READ_WRITE_COND_READN) {
                    assert (commit_ts >= access_ptr.timestamp_);
                    content_ref.SetTimestamp(commit_ts);
                } else if (access_ptr.access_type_ == INSERT_ONLY) {
                    assert (commit_ts >= access_ptr.timestamp_);
                    content_ref.SetTimestamp(commit_ts);
                } else if (access_ptr.access_type_ == DELETE_ONLY) {
                    assert (commit_ts >= access_ptr.timestamp_);
                    content_ref.SetTimestamp(commit_ts);
                }
            }
        }
        // release locks.
        for (int i = 0; i < access_list_.access_count_; ++i) {
            TxnAccess.Access access_ptr = access_list_.GetAccess(i);
            access_ptr.access_record_.content_.UnlockPartitions();
        }
        assert (access_list_.access_count_ <= kMaxAccessNum);
        access_list_.Clear();
        return true;
    }

    @Override
    public void AbortTransaction() {
        //not in use in this scheme.
    }

    // TODO: this is a bad encapsulation.
//    @Override
//    public void start_evaluate(int thread_Id, long mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
//        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
//        // add bid_array for events
//        if (thread_Id == 0) {
//            int partitionOffset = (int) (orderLock.sortedEvents.size() / thread_count_);
//            int[] p_bids = new int[(int) thread_count_];
//            HashMap<Integer, Integer> pids = new HashMap<>();
//            for (TxnEvent event : orderLock.sortedEvents) {
//                if (event instanceof TransactionEvent) {
//                    parseTransactionEvent(partitionOffset, (TransactionEvent) event, pids);
//                    event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
//                    pids.replaceAll((k, v) -> p_bids[k]++);
//                } else if (event instanceof DepositEvent) {
//                    parseDepositEvent(partitionOffset, pids, (DepositEvent) event);
//                    event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
//                    pids.replaceAll((k, v) -> p_bids[k]++);
//                } else {
//                    throw new UnsupportedOperationException();
//                }
//                pids.clear();
//            }
//            orderLock.sortedEvents.clear();
//        }
//        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
//    }
//
//    private void parseDepositEvent(int partitionOffset, HashMap<Integer, Integer> pids, DepositEvent event) {
//        pids.put((int) (Long.parseLong(event.getAccountId()) / partitionOffset), 0);
//    }
//
//    private void parseTransactionEvent(int partitionOffset, TransactionEvent event, HashMap<Integer, Integer> pids) {
//        pids.put((int) (Long.parseLong(event.getSourceAccountId()) / partitionOffset), 0);
//        pids.put((int) (Long.parseLong(event.getSourceBookEntryId()) / partitionOffset), 0);
//        pids.put((int) (Long.parseLong(event.getTargetAccountId()) / partitionOffset), 0);
//        pids.put((int) (Long.parseLong(event.getTargetBookEntryId()) / partitionOffset), 0);
//    }
}
