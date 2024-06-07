package intellistream.morphstream.engine.txn.scheduler.impl.ds;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.db.storage.datatype.StringDataBox;
import intellistream.morphstream.engine.db.storage.impl.RemoteStorageManager;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.OperationChain;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.TaskPrecedenceGraph;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.AppConfig;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class DSScheduler<Context extends DSContext> implements IScheduler<Context> {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DSScheduler.class);
    public final int delta;
    public final int totalThreads;
    public final TaskPrecedenceGraph<Context> tpg;
    public final RdmaWorkerManager rdmaWorkerManager;
    public final int managerId;
    public final RemoteStorageManager remoteStorageManager;
    public static final Client clientObj;
    static {
        try {
            Class<?> clientClass = Class.forName(MorphStreamEnv.get().configuration().getString("clientClassName"));
            clientObj = (Client) clientClass.getDeclaredConstructor().newInstance();
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    public DSScheduler(int totalThreads, int numItems, RdmaWorkerManager rdmaWorkerManager, RemoteStorageManager remoteStorageManager) {
        this.rdmaWorkerManager = rdmaWorkerManager;
        this.remoteStorageManager = remoteStorageManager;
        this.managerId = rdmaWorkerManager.getManagerId();
        this.totalThreads = totalThreads;
        this.delta = numItems / totalThreads;
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, numItems);
    }
    @Override
    public void initTPG(int offset) {
        tpg.initTPG();
    }
    @Override
    public void AddContext(int thisTaskId, Context context) {
        tpg.getThreadToContext().put(thisTaskId, context);
        tpg.setOCs(context);
    }

    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.push(request);
        return false;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.tempOperationMap.clear();
        context.requests.clear();
    }

    @Override
    public void TxnSubmitFinished(Context context, int batchID) {
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation operation = new Operation(request.write_key, request.table_name, request.txn_context, bid, request.accessType, request.condition_records.keySet(), request.function);
            context.tempOperationMap.put(request.function.getFunctionName(), operation);
        }
        this.tpg.setupOperations(context.tempOperationMap);
    }
    @Override
    public void INITIALIZE(Context context) {
        try {
            //Get ownership table from driver
            MeasureTools.WorkerRdmaRecvOwnershipTableStartEventTime(context.thisThreadId);
            this.remoteStorageManager.getOwnershipTable(this.rdmaWorkerManager, context);
            //Send remote operations to remote workers
            MeasureTools.WorkerRdmaRemoteOperationStartEventTime(context.thisThreadId);
            for (OperationChain oc : this.tpg.getThreadToOCs().get(context.thisThreadId)) {
                if (oc.operations.isEmpty()) {
                    continue;
                }
                int remoteWorkerId = this.remoteStorageManager.workerSideOwnershipTables.get(oc.getTableName()).getOwnershipWorkerId(oc.getPrimaryKey());
                if (remoteWorkerId != this.managerId) {
                    for (Operation op : oc.operations) {
                        this.rdmaWorkerManager.sendRemoteOperations(context.thisThreadId, remoteWorkerId, new FunctionMessage(op.getOperationRef()));
                    }
                    oc.setLocalState(false);
                    this.tpg.getRemoteOCs().add(oc);
                } else {
                    oc.setTempValue(this.remoteStorageManager.readLocalCache(oc.getTableName(), oc.getPrimaryKey(), this.managerId, context.thisThreadId));
                    oc.setLocalState(true);
                    this.tpg.getLocalOCs().add(oc);
                }
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            this.rdmaWorkerManager.sendRemoteOperationBatch(context.thisThreadId);
            //Receive remote operations from remote workers
            tpg.setupRemoteOperations(context.receiveRemoteOperations(rdmaWorkerManager));
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            MeasureTools.WorkerRdmaRemoteOperationEndEventTime(context.thisThreadId);
            MeasureTools.WorkerSetupDependenciesStartEventTime(context.thisThreadId);
            this.setLocalOCs(context);
            this.setRemoteOcs(context);
            context.setupDependencies();
            LOG.info("Finish initialize: " + context.thisThreadId + " with local ops :" + context.allocatedLocalTasks.size() + " with remote ops: " + context.allocatedRemoteTasks.size());
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            MeasureTools.WorkerSetupDependenciesEndEventTime(context.thisThreadId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void start_evaluation(Context context, long mark_ID, int num_events, int batchID) {
        LOG.info("Start evaluation: " + context.thisThreadId);
        INITIALIZE(context);
        MeasureTools.WorkerExecuteStartEventTime(context.thisThreadId);
        do {
            EXPLORE(context);
            PROCESS(context, mark_ID, batchID);
        } while (!FINISHED(context));
        RESET(context);
        LOG.info("Finish evaluation: " + context.thisThreadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        MeasureTools.WorkerExecuteEndEventTime(context.thisThreadId);
    }
    @Override
    public void EXPLORE(Context context) {
        context.next();
    }

    @Override
    public void PROCESS(Context context, long mark_ID, int batchID) {
        OperationChain oc = context.ready_oc;
        for (Operation op : oc.operations) {
            if (op.getOperationType().equals(MetaTypes.OperationStateType.EXECUTED)) {
                if (op.isReference) {
                    if (this.remoteStorageManager.checkOwnership(op.table_name, op.pKey, context.thisThreadId)) {
                        op.operationType = MetaTypes.OperationStateType.COMMITTED;
                        oc.setTempValue(this.remoteStorageManager.readLocalCache(oc.getTableName(), oc.getPrimaryKey(), this.managerId, context.thisThreadId));
                        oc.deleteOperation(op);
                        LOG.info("Commit reference operation");
                    } else {
                        break;
                    }
                } else {
                    op.tryToCommit(oc);
                }
            } else if (op.isReady()) {
                if (op.isReference) {
                    execute(op, oc);
                    break;
                } else {
                    execute(op, oc);
                    if (op.getOperationType().equals(MetaTypes.OperationStateType.READY)) {
                        break;
                    } else {
                        op.notifyChildren();
                        oc.deleteOperation(op);
                    }
                }
            } else {
                break;
            }
        }
    }


    @Override
    public boolean FINISHED(Context context) {
        return context.isFinished();
    }

    @Override
    public void RESET(Context context) {
        context.reset();
    }
    private void execute(Operation operation, OperationChain oc) {
        if (operation.isReference) {
            this.remoteStorageManager.updateOwnership(operation.table_name, operation.pKey, operation.sourceWorkerId, oc.getDsContext().thisThreadId);
            operation.operationType = MetaTypes.OperationStateType.EXECUTED;
            LOG.info("Reference operation from worker: " + operation.sourceWorkerId);
        } else {
            List<DataBox> dataBoxes = new ArrayList<>();
            StringDataBox stringDataBox = new StringDataBox();
            if (oc.isLocalState()) {
                stringDataBox.setString(oc.getTempValue().toString());
            } else {
                this.remoteStorageManager.asyncReadRemoteCache(this.rdmaWorkerManager, operation.table_name, operation.pKey, operation.remoteObject);
                if (operation.remoteObject.value != null) {
                    stringDataBox.setString(operation.remoteObject.value);
                    LOG.info("Read from remote cache with " +  oc.tryTimes + " times");
                    MeasureTools.WorkerRdmaRound(oc.getDsContext().thisThreadId, oc.tryTimes);
                    oc.tryTimes = 0;
                } else {
                    oc.tryTimes ++;
                    return;
                }
            }
            dataBoxes.add(stringDataBox);
            SchemaRecord readRecord = new SchemaRecord(dataBoxes);
            operation.function.getStateObject(operation.stateObjectName.get(0)).setSchemaRecord(readRecord);
            //UDF updates operation.udfResult, which is the value to be written to writeRecord
            boolean udfSuccess = clientObj.transactionUDF(operation.function);
            AppConfig.randomDelay();//To quantify the overhead of user-defined function
            if (udfSuccess) {
                if (operation.accessType == CommonMetaTypes.AccessType.WRITE
                        || operation.accessType == CommonMetaTypes.AccessType.WINDOW_WRITE
                        || operation.accessType == CommonMetaTypes.AccessType.NON_DETER_WRITE) {
                    //Update udf results to writeRecord
                    Object udfResult = operation.function.udfResult; //value to be written
                    oc.setTempValue(udfResult);
                    SchemaRecord tempo_record = new SchemaRecord(readRecord);
                    tempo_record.getValues().get(0).setString((String) udfResult);
                    //Assign updated schemaRecord back to stateAccess
                    operation.function.setUpdatedStateObject(tempo_record);
                    //Update State
                    if (oc.isLocalState()) {
                        oc.setTempValue(udfResult);
                    } else {
                        this.remoteStorageManager.syncWriteRemoteCache(this.rdmaWorkerManager, operation.table_name, operation.pKey, (String) udfResult);
                    }
                } else if (operation.accessType == CommonMetaTypes.AccessType.READ && !oc.isLocalState()) {
                    this.remoteStorageManager.syncWriteRemoteCache(this.rdmaWorkerManager, operation.table_name, operation.pKey, stringDataBox.getString());
                }
                operation.operationType = MetaTypes.OperationStateType.EXECUTED;
            } else {
                operation.function.setAborted();
                operation.operationType = MetaTypes.OperationStateType.ABORTED;
                operation.notifyChildren();
            }
        }
    }

    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        throw new UnsupportedOperationException();
    }
    public void setLocalOCs(Context context) {
        int totalLocalOps = tpg.getLocalOCs().size();
        int delta = totalLocalOps / totalThreads;
        int leftBound = delta * context.thisThreadId;
        int rightBound;
        if (context.thisThreadId == totalThreads - 1) {
            rightBound = totalLocalOps;
        } else {
            rightBound = delta * (context.thisThreadId + 1);
        }
        int i = 0;
        for (OperationChain oc : tpg.getLocalOCs()) {
            if (i >= leftBound && i < rightBound) {
                oc.setDsContext(context);
                context.addTasks(oc);
            } else if (i > rightBound) {
                break;
            }
            i ++;
        }
    }
    public void setRemoteOcs(Context context) {
        int totalRemoteOps = tpg.getRemoteOCs().size();
        int delta = totalRemoteOps / totalThreads;
        int leftBound = delta * context.thisThreadId;
        int rightBound;
        if (context.thisThreadId == totalThreads - 1) {
            rightBound = totalRemoteOps;
        } else {
            rightBound = delta * (context.thisThreadId + 1);
        }
        int i = 0;
        for (OperationChain oc : tpg.getRemoteOCs()) {
            if (i >= leftBound && i < rightBound) {
                oc.setDsContext(context);
                context.addTasks(oc);
            } else if (i > rightBound) {
                break;
            }
            i ++;
        }
    }
}
