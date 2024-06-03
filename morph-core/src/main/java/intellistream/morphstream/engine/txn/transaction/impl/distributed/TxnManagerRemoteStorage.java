package intellistream.morphstream.engine.txn.transaction.impl.distributed;

import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.db.storage.record.SchemaRecordRef;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.lock.OrderLock;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.SchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.scheduler.context.ds.OCCContext;
import intellistream.morphstream.engine.txn.scheduler.context.ds.RLContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGNSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGNSContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPNSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPNSContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPSAContext;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPSContext;
import intellistream.morphstream.engine.txn.scheduler.context.recovery.RSContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.impl.recovery.RScheduler;
import intellistream.morphstream.engine.txn.transaction.TxnManager;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

public class TxnManagerRemoteStorage extends TxnManager {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerRemoteStorage.class);
    protected int thread_count_;
    public HashMap<String, SchedulerContext> contexts;
    public SchedulerContext context;
    protected int dalta;
    public TxnManagerRemoteStorage(int thisTaskId, int thread_count, String schedulerType) {
        this.thread_count_ = thread_count;
        contexts = new HashMap<>();
        if (enableGroup) {
            dalta = (int) Math.ceil(thread_count / (double) groupNum);
            this.setSchedulerContext(thisTaskId, thread_count / groupNum, schedulerTypeByGroup.get(thisTaskId / dalta), schedulerByGroup.get(thisTaskId / dalta));
            context = contexts.get(schedulerTypeByGroup.get(thisTaskId / dalta));
        } else if (enableDynamic) {
            for (Map.Entry<String, IScheduler> entry : schedulerPool.entrySet()) {
                this.setSchedulerContext(thisTaskId, thread_count, entry.getKey(), entry.getValue());
            }
            context = contexts.get(schedulerType);
        } else {
            this.setSchedulerContext(thisTaskId, thread_count, schedulerType, scheduler);
            context = contexts.get(schedulerType);
        }
        if (recoveryScheduler != null) {
            this.setSchedulerContext(thisTaskId, thread_count, "Recovery", recoveryScheduler);
            context = contexts.get("Recovery");
        }
    }
    public void setSchedulerContext(int thisTaskId, int thread_count, String schedulerType, IScheduler scheduler) {
        SCHEDULER_TYPE scheduler_type = SCHEDULER_TYPE.valueOf(schedulerType);
        SchedulerContext schedulerContext;
        switch (scheduler_type) {
            case OG_BFS:
            case OG_DFS:
                schedulerContext = new OGSContext(thisTaskId, thread_count);
                break;
            case OG_BFS_A:
            case OG_DFS_A:
                schedulerContext = new OGSAContext(thisTaskId, thread_count);
                break;
            case OG_NS:
            case TStream: // original tstream is the same as using GS scheduler..
                schedulerContext = new OGNSContext(thisTaskId, thread_count);
                break;
            case OG_NS_A:
                schedulerContext = new OGNSAContext(thisTaskId, thread_count);
                break;
            case OP_NS:
                schedulerContext = new OPNSContext(thisTaskId);
                break;
            case OP_NS_A:
                schedulerContext = new OPNSAContext(thisTaskId);
                break;
            case OP_BFS:
            case OP_DFS:
                schedulerContext = new OPSContext(thisTaskId);
                break;
            case OP_BFS_A:
            case OP_DFS_A:
                schedulerContext = new OPSAContext(thisTaskId);
                break;
            case Recovery:
                schedulerContext = new RSContext(thisTaskId);
                break;
            case DScheduler:
                schedulerContext = new DSContext(thisTaskId);
                break;
            case RLScheduler:
                schedulerContext = new RLContext(thisTaskId);
                break;
            case OCCScheduler:
                schedulerContext = new OCCContext(thisTaskId);
                break;
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + scheduler_type);
        }
        contexts.put(schedulerType, schedulerContext);
        scheduler.AddContext(thisTaskId, schedulerContext);
    }

    @Override
    public void start_evaluate(String operatorID, int batchID, int num_events, int taskId, long mark_ID) throws InterruptedException, BrokenBarrierException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean submitStateAccess(Function function, FunctionContext functionContext) throws DatabaseException {
        MetaTypes.AccessType accessType = function.getAccessType();
        if (accessType == MetaTypes.AccessType.READ) {
            return Asy_ReadRecord(function, functionContext);
        } else if (accessType == MetaTypes.AccessType.WRITE) {
            return Asy_WriteRecord(function, functionContext);
        } else {
            throw new UnsupportedOperationException("Unsupported access type: " + accessType);
        }
    }
    @Override
    public void BeginTransaction(FunctionContext txn_context) {
        if (enableGroup) {
            schedulerByGroup.get(getGroupId(txn_context.thread_Id)).TxnSubmitBegin(context);
        } else {
            scheduler.TxnSubmitBegin(context);
        }
    }
    @Override
    public boolean CommitTransaction(FunctionContext txn_context, int batchID) {
        if (enableGroup) {
            schedulerByGroup.get(getGroupId(txn_context.thread_Id)).TxnSubmitFinished(context, batchID);
        } else {
            scheduler.TxnSubmitFinished(context, batchID);
        }
        return true;
    }
    private boolean Asy_ReadRecord(Function function, FunctionContext functionContext) {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ;
        List<StateObject> stateObjects = new ArrayList<>(function.getStateObjects());
        if (stateObjects.size() != 1) {
            throw new UnsupportedOperationException("Read only supports single read access.");
        }
        StateObject stateObj = stateObjects.get(0);
        String srcTable = stateObj.getTable();
        String srcKey = stateObj.getKey();
        HashMap<String, TableRecord> condition_records = new HashMap<>();
        condition_records.put(stateObj.getName(), null);

        if (enableGroup) {
            schedulerByGroup.get(getGroupId(functionContext.thread_Id)).SubmitRequest(context,
                    new Request(functionContext, accessType, srcTable, srcKey, condition_records, function));
        } else {
            scheduler.SubmitRequest(context, new Request(functionContext, accessType, srcTable,
                    srcKey, condition_records, function));
        }
        return false;
    }
    private boolean Asy_WriteRecord(Function function, FunctionContext functionContext) {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WRITE;
        List<StateObject> stateObjects = new ArrayList<>(function.getStateObjects());
        HashMap<String, TableRecord> condition_records = new HashMap<>();

        for (StateObject stateObject : stateObjects) {
            condition_records.put(stateObject.getName(), null);
        }
        StateObject writeStateObj = function.getStateObjectToWrite();
        String writeTable = writeStateObj.getTable();
        String writeKey = writeStateObj.getKey();

        if (enableGroup) {
            schedulerByGroup.get(getGroupId(functionContext.thread_Id)).SubmitRequest(context,
                    new Request(functionContext, accessType, writeTable,
                            writeKey, condition_records, function));
        } else {
            scheduler.SubmitRequest(context, new Request(functionContext, accessType, writeTable,
                    writeKey, condition_records, function));
        }
        return false;
    }

    @Override
    public boolean InsertRecord(FunctionContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    public void SwitchScheduler(String schedulerType, int threadId, long markId, int batchID, String operatorID) {
        currentSchedulerType.put(threadId, schedulerType);
        if (threadId == 0) {
            scheduler = schedulerPool.get(schedulerType);
            LOG.info("Current Scheduler is " + schedulerType + " markId: " + markId);
        }
    }
    public void switchContext(String schedulerType) {
        context = contexts.get(schedulerType);
    }
    @Override
    public void switch_scheduler(int thread_Id, long mark_ID, int batchID, String operatorID) {
        if (scheduler instanceof RScheduler) {
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
            String schedulerType = collector.getDecision(thread_Id);
            this.SwitchScheduler(schedulerType, thread_Id, mark_ID, batchID, operatorID);
            this.switchContext(schedulerType);
            SOURCE_CONTROL.getInstance().waitForSchedulerSwitch(thread_Id);
        }
    }

    @Override
    public boolean SelectKeyRecord(FunctionContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException, InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean lock_ahead(FunctionContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean SelectKeyRecord_noLock(FunctionContext txn_context, String table_name, String key, SchemaRecordRef record_ref, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean lock_all(SpinLock[] spinLocks) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean unlock_all(SpinLock[] spinLocks) throws DatabaseException {
        throw new UnsupportedOperationException();
    }

    @Override
    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PartitionedOrderLock.LOCK getOrderLock(int p_id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SchedulerContext getSchedulerContext() {
        throw new UnsupportedOperationException();
    }
    public int getGroupId(int thisTaskId) {
        int groupId = thisTaskId / dalta;
        return groupId;
    }
}
