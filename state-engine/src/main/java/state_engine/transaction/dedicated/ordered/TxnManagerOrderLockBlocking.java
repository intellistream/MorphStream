package state_engine.transaction.dedicated.ordered;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.DatabaseException;
import state_engine.Meta.MetaTypes;
import state_engine.common.OrderLock;
import state_engine.content.Content;
import state_engine.storage.SchemaRecord;
import state_engine.storage.SchemaRecordRef;
import state_engine.storage.StorageManager;
import state_engine.storage.TableRecord;
import state_engine.transaction.dedicated.TxnManagerDedicated;
import state_engine.transaction.impl.TxnContext;

import java.util.LinkedList;

import static state_engine.Meta.MetaTypes.AccessType.*;
import static state_engine.Meta.MetaTypes.kMaxAccessNum;
import static state_engine.transaction.impl.TxnAccess.Access;

/**
 * mimic of ACEP's S2PL method. It is essentially a blocking-based order locking.
 */
public class TxnManagerOrderLockBlocking extends TxnManagerDedicated {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerOrderLockBlocking.class);
    public final OrderLock orderLock;

    public TxnManagerOrderLockBlocking(StorageManager storageManager, String thisComponentId, int thisTaskId, int thread_count) {
        super(storageManager, thisComponentId, thisTaskId, thread_count);

        this.orderLock = OrderLock.getInstance();
    }

    public OrderLock getOrderLock() {
        return orderLock;
    }

    @Override
    public boolean InsertRecord(TxnContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap)
            throws DatabaseException, InterruptedException {
        record.is_visible_ = false;
        TableRecord tb_record = new TableRecord(record);
        if (storageManager_.getTable(table_name).InsertRecord(tb_record)) {//maybe we can also skip this for testing purpose.
            orderLock.blocking_wait(txn_context.getBID());
            while (!tb_record.content_.TryWriteLock()) {//order guaranteed...
                txn_context.is_retry_ = true;//retry, no abort..
            }
            orderLock.advance();
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
    protected boolean lock_aheadCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) {
        record_ref.setRecord(t_record.record_);//Note that, locking scheme allows directly modifying on original table d_record.

        if (accessType == READ_ONLY) {
            //The following makes sure the lock_ratio is added in event sequence as in ACEP.

            while (!t_record.content_.TryReadLock()) {
                txn_context.is_retry_ = true;//retry, no abort..
            }

            return true;

        } else if (accessType == READ_WRITE) {


            while (!t_record.content_.TryWriteLock() && !Thread.currentThread().isInterrupted()) {
                txn_context.is_retry_ = true;
            }

            return true;

        } else if (accessType == DELETE_ONLY) {

            while (!t_record.content_.TryWriteLock() && !Thread.currentThread().isInterrupted()) {
                txn_context.is_retry_ = true;
            }

            return true;
        } else {
            assert (false);
            return false;
        }
    }

    @Override
    public boolean SelectKeyRecord_noLockCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) {
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

            LOG.info(t_record.toString() + "is locked by deleter");
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
    protected boolean SelectRecordCC(TxnContext txn_context, String table_name, TableRecord t_record, SchemaRecordRef record_ref, MetaTypes.AccessType accessType) {
        record_ref.setRecord(t_record.record_);//Note that, locking scheme allows directly modifying on original table d_record.

        if (accessType == READ_ONLY) {
            //The following makes sure the lock_ratio is added in event sequence as in ACEP.


            while (!t_record.content_.TryReadLock()) {
                txn_context.is_retry_ = true;//retry, no abort..
            }

            Access access = access_list_.NewAccess();
            access.access_type_ = READ_ONLY;
            access.access_record_ = t_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();

            return true;

        } else if (accessType == READ_WRITE) {

            while (!t_record.content_.TryWriteLock()) {
                txn_context.is_retry_ = true;
            }

            //LOG.info(txn_context.thisTaskId + " success to get orderLock" + DateTime.now());
            final SchemaRecord local_record = new SchemaRecord(t_record.record_);//copy from t_record to local_record.
            Access access = access_list_.NewAccess();
            access.access_type_ = READ_WRITE;
            access.access_record_ = t_record;
            access.local_record_ = local_record;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();

            return true;

        } else if (accessType == DELETE_ONLY) {
//            orderLock.blocking_wait(txn_context.getBID());
            while (!t_record.content_.TryWriteLock()) {
                txn_context.is_retry_ = true;
            }
//            orderLock.advance();

            LOG.info(t_record.toString() + "is locked by deleter");
            t_record.record_.is_visible_ = false;
            Access access = access_list_.NewAccess();
            access.access_type_ = DELETE_ONLY;
            access.access_record_ = t_record;
            access.local_record_ = null;
            access.table_id_ = table_name;
            access.timestamp_ = t_record.content_.GetTimestamp();

            return true;
        } else {
            assert (false);
            return false;
        }
    }


    @Override
    public boolean CommitTransaction(TxnContext txn_context) {


//		long curr_epoch = Epoch.GetEpoch();
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

        // release locks.
        for (int i = 0; i < access_list_.access_count_; ++i) {
            Access access_ptr = access_list_.GetAccess(i);
            if (access_ptr.access_type_ == READ_ONLY) {
                access_ptr.access_record_.content_.ReleaseReadLock();
            } else if (access_ptr.access_type_ == READ_WRITE) {
//                SchemaRecord local_record_ptr = access_ptr.local_record_;
                access_ptr.access_record_.content_.ReleaseWriteLock();
//                local_record_ptr.clean();
            } else {
                // insert_only or delete_only
                access_ptr.access_record_.content_.ReleaseWriteLock();
            }
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
