package intellistream.morphstream.engine.txn.transaction.impl;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.MetaTypes;
import intellistream.morphstream.engine.db.exception.DatabaseException;
import intellistream.morphstream.engine.db.storage.StorageManager;
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

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public abstract class TxnManagerDedicatedAsyDistributed extends TxnManager {
    private static final Logger LOG = LoggerFactory.getLogger(TxnManagerDedicatedAsyDistributed.class);
    protected int thread_count_;
    public HashMap<String, SchedulerContext> contexts;
    public SchedulerContext context;
    protected int dalta;
    public TxnManagerDedicatedAsyDistributed(int thisTaskId, int thread_count, String schedulerType) {
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
    @Override
    public boolean submitStateAccess(StateAccess stateAccess, FunctionContext functionContext) throws DatabaseException {
        MetaTypes.AccessType accessType = stateAccess.getAccessType();
        if (accessType == MetaTypes.AccessType.READ) {
            return Asy_ReadRecord(stateAccess, functionContext);
        } else if (accessType == MetaTypes.AccessType.WRITE) {
            return Asy_WriteRecord(stateAccess, functionContext);
        } else {
            throw new UnsupportedOperationException("Unsupported access type: " + accessType);
        }
    }
    private boolean Asy_ReadRecord(StateAccess stateAccess, FunctionContext functionContext) {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.READ;
        List<StateObject> stateObjects = new ArrayList<>(stateAccess.getStateObjects());
        if (stateObjects.size() != 1) {
            throw new UnsupportedOperationException("Read only supports single read access.");
        }
        StateObject stateObj = stateObjects.get(0);
        String srcTable = stateObj.getTable();
        String srcKey = stateObj.getKey();
        String[] condition_sourceTables = {srcTable};
        String[] condition_sourceKeys = {srcKey};

        if (enableGroup) {
            schedulerByGroup.get(getGroupId(functionContext.thread_Id)).SubmitRequest(context,
                    new Request(functionContext, accessType, srcTable,
                    srcKey, condition_sourceTables, condition_sourceKeys, stateAccess));
        } else {
            scheduler.SubmitRequest(context, new Request(functionContext, accessType, srcTable,
                    srcKey, condition_sourceTables, condition_sourceKeys, stateAccess));
        }
        return false;
    }
    private boolean Asy_WriteRecord(StateAccess stateAccess, FunctionContext functionContext) {
        CommonMetaTypes.AccessType accessType = CommonMetaTypes.AccessType.WRITE;
        List<StateObject> stateObjects = new ArrayList<>(stateAccess.getStateObjects());

        String[] condition_tables = new String[stateAccess.getStateObjects().size()];
        String[] condition_keys = new String[stateAccess.getStateObjects().size()];

        for (int i = 0; i < stateObjects.size(); i++) {
            StateObject stateObj = stateObjects.get(i);
            String stateObjTable = stateObj.getTable();
            String stateObjKey = stateObj.getKey();

            condition_tables[i] = stateObjTable;
            condition_keys[i] = stateObjKey;
        }

        StateObject writeStateObj = stateAccess.getStateObjectToWrite();
        String writeTable = writeStateObj.getTable();
        String writeKey = writeStateObj.getKey();

        if (enableGroup) {
            schedulerByGroup.get(getGroupId(functionContext.thread_Id)).SubmitRequest(context,
                    new Request(functionContext, accessType, writeTable,
                            writeKey, condition_tables, condition_keys, stateAccess));
        } else {
            scheduler.SubmitRequest(context, new Request(functionContext, accessType, writeTable,
                    writeKey, condition_tables, condition_keys, stateAccess));
        }
        return false;
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
            //TODO: Replace with the following code for stage
//            stage.getScheduler().TxnSubmitFinished(context);
        } else {
            scheduler.TxnSubmitFinished(context, batchID);
        }
        return true;
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
            default:
                throw new UnsupportedOperationException("unsupported scheduler type: " + scheduler_type);
        }
        contexts.put(schedulerType, schedulerContext);
        scheduler.AddContext(thisTaskId, schedulerContext);
    }
    @Override
    public SchedulerContext getSchedulerContext() {
        return context;
    }
    /**
     * Switch scheduler every punctuation
     * When the workload changes and the scheduler is no longer applicable
     */
    public void switchContext(String schedulerType) {
        context = contexts.get(schedulerType);
    }
    public void SwitchScheduler(String schedulerType, int threadId, long markId, int batchID, String operatorID) {
        currentSchedulerType.put(threadId, schedulerType);
        if (threadId == 0) {
            scheduler = schedulerPool.get(schedulerType);
            LOG.info("Current Scheduler is " + schedulerType + " markId: " + markId);
        }
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
    public OrderLock getOrderLock() {
        throw new UnsupportedOperationException();
    }
    @Override
    public PartitionedOrderLock.LOCK getOrderLock(int pid) {
        throw new UnsupportedOperationException();
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
    public boolean InsertRecord(FunctionContext txn_context, String table_name, SchemaRecord record, LinkedList<Long> gap) throws DatabaseException, InterruptedException {
        return false;
    }
    public int getGroupId(int thisTaskId) {
        int groupId = thisTaskId / dalta;
        return groupId;
    }
}
