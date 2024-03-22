package intellistream.morphstream.engine.txn.scheduler.impl.ds;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.input.FunctionMessage;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.common.io.Rdma.RdmaWorkerManager;
import intellistream.morphstream.engine.db.storage.datatype.IntDataBox;
import intellistream.morphstream.engine.db.storage.impl.RemoteStorageManager;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
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
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Random;

public class DSSchedule<Context extends DSContext> implements IScheduler<Context> {
    private static final Logger LOG = Logger.getLogger(DSSchedule.class);
    public final int delta;
    public final int totalThreads;
    public final TaskPrecedenceGraph<Context> tpg;
    public final RdmaWorkerManager rdmaWorkerManager;
    public final int managerId;
    public final Random signatureRandom = new Random();
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
    public DSSchedule(int totalThreads, int numItems, RdmaWorkerManager rdmaWorkerManager, RemoteStorageManager remoteStorageManager) {
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
            Operation operation = new Operation(request.write_key, request.table_name, request.txn_context, bid, request.accessType, (ArrayList<String>) request.condition_records.keySet(), request.stateAccess);
            context.tempOperationMap.put(request.stateAccess.getStateAccessName(), operation);
        }
        this.tpg.setupOperations(context.tempOperationMap);
    }
    @Override
    public void INITIALIZE(Context context) {
        try {
            //Get ownership table from driver
            this.remoteStorageManager.getOwnershipTable(this.rdmaWorkerManager, context);
            //Send remote operations to remote workers
            for (OperationChain oc : this.tpg.getThreadToOCs().get(context.thisThreadId)) {
                int remoteWorkerId = this.remoteStorageManager.workerSideOwnershipTable.getOwnershipWorkerId(oc.getPrimaryKey());
                if (remoteWorkerId != this.managerId) {
                    for (Operation op : oc.operations) {
                        this.rdmaWorkerManager.sendRemoteOperations(context.thisThreadId, remoteWorkerId, new FunctionMessage(op.getOperationRef()));
                    }
                    oc.setLocalState(false);
                } else {
                    oc.setTempValue(this.remoteStorageManager.readLocalCache(oc.getTableName(), oc.getPrimaryKey(),
                            this.managerId, signatureRandom.nextInt()));
                    oc.setLocalState(true);
                }
            }
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            this.rdmaWorkerManager.sendRemoteOperationBatch(context.thisThreadId);
            //Receive remote operations from remote workers
            tpg.setupRemoteOperations(context.receiveRemoteOperations(rdmaWorkerManager));
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
            tpg.setupDependencies(context);
            for (OperationChain oc : this.tpg.getThreadToOCs().get(context.thisThreadId)) {
                oc.setDsContext(context);
                context.addTasks(oc);
            }
            LOG.info("Finish initialize: " + context.thisThreadId);
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void start_evaluation(Context context, long mark_ID, int num_events, int batchID) {
        LOG.info("Start evaluation: " + context.thisThreadId);
        INITIALIZE(context);
        do {
            EXPLORE(context);
            PROCESS(context, mark_ID, batchID);
        } while (!FINISHED(context));
        RESET(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
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
                    if (this.remoteStorageManager.checkOwnership(op.table_name, op.pKey)) {
                        op.operationType = MetaTypes.OperationStateType.COMMITTED;
                    }
                } else {
                    op.tryToCommit(oc);
                }
            } else if (op.isReady()) {
                if (op.earlyAbort()) {
                    oc.deleteOperation(op);
                    continue;
                } else {
                    execute(op, oc);
                }
                if (op.getOperationType().equals(MetaTypes.OperationStateType.ABORTED)) {
                    oc.deleteOperation(op);
                } else if (op.getOperationType().equals(MetaTypes.OperationStateType.EXECUTED)) {
                    op.tryToCommit(oc);
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
            this.remoteStorageManager.updateOwnership(operation.table_name, operation.pKey, operation.sourceWorkerId);
            operation.operationType = MetaTypes.OperationStateType.EXECUTED;
        } else {
            IntDataBox intDataBox = new IntDataBox();
            if (oc.isLocalState()) {
                intDataBox.setInt((Integer) oc.getTempValue());
            } else {
                int signature = signatureRandom.nextInt();
                int value = this.remoteStorageManager.syncReadRemoteCache(this.rdmaWorkerManager, operation.table_name, operation.pKey, signature);
                if (value == signature) {
                    return;
                } else {
                    intDataBox.setInt(value);
                }
            }
            SchemaRecord readRecord = new SchemaRecord(intDataBox);
            operation.stateAccess.getStateObject(operation.stateObjectName.get(0)).setSchemaRecord(readRecord);
            //UDF updates operation.udfResult, which is the value to be written to writeRecord
            boolean udfSuccess = false;
            udfSuccess = clientObj.transactionUDF(operation.stateAccess);
            AppConfig.randomDelay();//To quantify the overhead of user-defined function
            if (udfSuccess) {
                if (operation.accessType == CommonMetaTypes.AccessType.WRITE
                        || operation.accessType == CommonMetaTypes.AccessType.WINDOW_WRITE
                        || operation.accessType == CommonMetaTypes.AccessType.NON_DETER_WRITE) {
                    //Update udf results to writeRecord
                    Object udfResult = operation.stateAccess.udfResult; //value to be written
                    oc.setTempValue(udfResult);
                    SchemaRecord tempo_record = new SchemaRecord(readRecord);
                    tempo_record.getValues().get(1).setInt((int) udfResult);
                    //Assign updated schemaRecord back to stateAccess
                    operation.stateAccess.setUpdatedStateObject(tempo_record);
                } else {
                    throw new UnsupportedOperationException();
                }
                operation.operationType = MetaTypes.OperationStateType.EXECUTED;
            } else {
                operation.stateAccess.setAborted();
                operation.operationType = MetaTypes.OperationStateType.ABORTED;
                operation.notifyChildren();
            }
        }
    }

    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        throw new UnsupportedOperationException();
    }
}
