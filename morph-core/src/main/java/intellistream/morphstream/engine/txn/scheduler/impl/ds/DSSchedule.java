package intellistream.morphstream.engine.txn.scheduler.impl.ds;

import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.ds.DSContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.OperationChain;
import intellistream.morphstream.engine.txn.scheduler.struct.ds.TaskPrecedenceGraph;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.AppConfig;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class DSSchedule<Context extends DSContext> implements IScheduler<Context> {
    private static final Logger LOG = Logger.getLogger(DSSchedule.class);
    public final int delta;
    public final int totalThreads;
    public final TaskPrecedenceGraph<Context> tpg;
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
    public DSSchedule(int totalThreads, int numItems) {
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
            Operation operation = new Operation(request.write_key, null, request.table_name, request.txn_context, bid, request.accessType, request.d_record, request.condition_records, request.stateAccess);
            context.tempOperationMap.put(request.stateAccess.getStateAccessName(), operation);
        }
        this.tpg.setupOperations(context.tempOperationMap);
    }
    @Override
    public void INITIALIZE(Context context) {
        tpg.setupDependencies(context);
        for (OperationChain oc : this.tpg.getThreadToOCs().get(context.thisThreadId)) {
            oc.setDsContext(context);
            context.addTasks(oc);
        }
        LOG.info("Finish initialize: " + context.thisThreadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
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
                op.tryToCommit(oc);
            } else if (op.isReady()) {
                if (op.earlyAbort()) {
                    oc.deleteOperation(op);
                    continue;
                } else {
                    execute(op, mark_ID);
                }
                if (op.getOperationType().equals(MetaTypes.OperationStateType.ABORTED)) {
                    oc.deleteOperation(op);
                } else {
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
    private void execute(Operation operation, long mark_ID) {
        AppConfig.randomDelay();//To quantify the overhead of user-defined function
        //Before executing udf, read schemaRecord from tableRecord and write into stateAccess. Applicable to all 6 types of operations.
        for (Map.Entry<String, TableRecord> entry : operation.condition_records.entrySet()) {
            SchemaRecord readRecord = entry.getValue().content_.readPreValues(operation.bid);
            operation.stateAccess.getStateObject(entry.getKey()).setSchemaRecord(readRecord);
        }

        //UDF updates operation.udfResult, which is the value to be written to writeRecord
        boolean udfSuccess = false;
        udfSuccess = clientObj.transactionUDF(operation.stateAccess);

        if (udfSuccess) {
            if (operation.accessType == CommonMetaTypes.AccessType.WRITE
                    || operation.accessType == CommonMetaTypes.AccessType.WINDOW_WRITE
                    || operation.accessType == CommonMetaTypes.AccessType.NON_DETER_WRITE) {
                //Update udf results to writeRecord
                Object udfResult = operation.stateAccess.udfResult; //value to be written
                SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
                SchemaRecord tempo_record = new SchemaRecord(srcRecord);
                tempo_record.getValues().get(1).setDouble((double) udfResult);
                operation.d_record.content_.updateMultiValues(operation.bid, mark_ID, false, tempo_record);
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

    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        throw new UnsupportedOperationException();
    }
}
