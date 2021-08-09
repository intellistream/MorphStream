package transaction.dedicated.ordered;

import common.OrderLock;
import common.PartitionedOrderLock;
import common.meta.CommonMetaTypes;
import content.Content;
import db.DatabaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.StorageManager;
import storage.TableRecord;
import transaction.dedicated.TxnManagerDedicatedLocked;
import transaction.impl.TxnContext;

import java.util.LinkedList;

import static common.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;
import static common.meta.CommonMetaTypes.AccessType.*;
import static common.meta.CommonMetaTypes.kMaxAccessNum;
import static transaction.impl.TxnAccess.Access;
import static utils.PartitionHelper.key_to_partition;

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
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap)
            throws DatabaseException {
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            while (!tb_record.content_.TryWriteLock()) {
                txn_context.is_retry_ = true;//retry, no abort..
            }
            record.is_visible_ = true;
            Access access = access_list_.NewAccess();
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
//        LOG.info("LOCK FOR:" + t_record.record_.getValues().get(0)+" pid:"+txn_context.pid);
        t_record.content_.LockPartitions();//it should always success.
        return true;
    }

    @Override
    public boolean SelectKeyRecord_noLockCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) {
        record_ref.setRecord(t_record.record_);//Note that, locking scheme allows directly modifying on original table d_record.
        if (accessType == READ_ONLY) {
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_ONLY;
            access.access_record_ = t_record;
//            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            return true;
        } else if (accessType == READ_WRITE) {
            //LOG.info(txn_context.thisTaskId + " success to get orderLock" + DateTime.now());
//            final SchemaRecord local_record = new SchemaRecord(t_record.record_);//copy from t_record to local_record.
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_WRITE;
            access.access_record_ = t_record;
//            access.local_record_ = local_record;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
            return true;
        } else if (accessType == DELETE_ONLY) {
            //LOG.info(t_record.toString() + "is locked by deleter");
            t_record.record_.is_visible_ = false;
            Access access = access_list_.NewAccess();
            access.access_type_ = DELETE_ONLY;
            access.access_record_ = t_record;
//            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();
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
                Access access_ptr = access_list_.GetAccess(i);
                Content content_ref = access_ptr.access_record_.content_;
                if (access_ptr.access_type_ == READ_WRITE) {
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
                Access access_ptr = access_list_.GetAccess(i);
                Content content_ref = access_ptr.access_record_.content_;
                if (access_ptr.access_type_ == READ_WRITE) {
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
            Access access_ptr = access_list_.GetAccess(i);
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
}
