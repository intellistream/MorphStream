package intellistream.morphstream.engine.txn.scheduler.impl.recovery;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.PathLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.durability.struct.FaultToleranceRelax;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.recovery.RSContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.AbstractOperation;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.recovery.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.recovery.OperationChain;
import intellistream.morphstream.engine.txn.scheduler.struct.recovery.TaskPrecedenceGraph;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.datatype.DoubleDataBox;
import intellistream.morphstream.engine.txn.storage.datatype.IntDataBox;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static intellistream.morphstream.common.constants.TPConstants.Constant.MAX_INT;
import static intellistream.morphstream.common.constants.TPConstants.Constant.MAX_SPEED;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.*;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.defaultString;
import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_no;
import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_path;

public class RScheduler<Context extends RSContext> implements IScheduler<Context> {
    private static final Logger LOG = LoggerFactory.getLogger(RScheduler.class);
    public final int delta;
    public final TaskPrecedenceGraph<Context> tpg;
    public AtomicBoolean abortHandling = new AtomicBoolean(false);
    public int isLogging;
    public LoggingManager loggingManager;
    String appName;

    public RScheduler(int totalThreads, int NUM_ITEMS) {
        this.delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads);
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, delta, NUM_ITEMS);
        this.appName = MorphStreamEnv.get().configuration().getString("app");
    }

    public static int getTaskId(String key, Integer delta) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }

    @Override
    public void initTPG(int offset) {
        tpg.initTPG(offset);
    }

    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        this.loggingManager = loggingManager;
        if (loggingManager instanceof PathLoggingManager) {
            isLogging = LOGOption_path;
            this.tpg.threadToPathRecord = ((PathLoggingManager) loggingManager).threadToPathRecord;
        } else {
            isLogging = LOGOption_no;
        }
        this.tpg.isLogging = this.isLogging;
    }

    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.push(request);
        return false;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        if (context.needCheckId) {
            context.needCheckId = false;
            context.groupId = this.loggingManager.getHistoryViews().checkGroupId(context.groupId);
        }
        context.requests.clear();
    }

    @Override
    public void TxnSubmitFinished(Context context, int batchID) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        int txnOpId = 0;
        for (Request request : context.requests) {
            long bid = request.txn_context.getBID();
            Operation set_op;
            switch (request.accessType) {
                case WRITE_ONLY:
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, null, request.stateAccess);
                    break;
                case READ_WRITE: // they can use the same method for processing
                case READ_WRITE_COND:
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, request.stateAccess);
                    break;
                case READ_WRITE_COND_READ:
                case READ_WRITE_COND_READN:
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, request.stateAccess);
                    break;
                case READ_WRITE_READ:
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, null, request.stateAccess);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            OperationChain curOC = tpg.addOperationToChain(set_op);
            set_op.setTxnOpId(txnOpId++);
            if (request.condition_keys != null) {
                inspectDependency(context.groupId, curOC, set_op, request.table_name, request.write_key, request.condition_tables, request.condition_keys);
            }
            MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        }
    }

    @Override
    public void AddContext(int threadId, Context context) {
        tpg.threadToContextMap.put(threadId, context);
        tpg.setOCs(context);
    }

    @Override
    public void INITIALIZE(Context context) {
        if (FaultToleranceRelax.isTaskPlacing) {
            HashMap<String, List<Integer>> plan;
            if (!this.loggingManager.getHistoryViews().canInspectTaskPlacing(context.groupId)) {
                this.graphConstruct(context);
            }
            plan = this.loggingManager.inspectTaskPlacing(context.groupId, context.thisThreadId);
            for (Map.Entry<String, List<Integer>> entry : plan.entrySet()) {
                String table = entry.getKey();
                List<Integer> value = entry.getValue();
                for (int key : value) {
                    int taskId = getTaskId(String.valueOf(key), delta);
                    OperationChain oc = tpg.getOperationChains().get(table).threadOCsMap.get(taskId).holder_v1.get(String.valueOf(key));
                    context.allocatedTasks.add(oc);
                    context.totalTasks = context.totalTasks + oc.operations.size();
                }
            }
        } else {
            for (OperationChain oc : tpg.threadToOCs.get(context.thisThreadId)) {
                context.allocatedTasks.add(oc);
                context.totalTasks = context.totalTasks + oc.operations.size();
            }
        }
    }

    @Override
    public void PROCESS(Context context, long mark_ID, int batchID) {
        OperationChain oc = context.ready_oc;
        for (Operation op : oc.operations) {
            if (op.pKey.equals(oc.getPrimaryKey())) {
                if (op.operationState.equals(MetaTypes.OperationStateType.EXECUTED) || op.operationState.equals(MetaTypes.OperationStateType.ABORTED)) {
                    continue;
                }
                if (op.pdCount.get() == 0) {
                    MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
                    execute(op, mark_ID, false);
                    if (op.isFailed.get()) {
                        op.operationState = MetaTypes.OperationStateType.ABORTED;
                        checkAbort();
                    } else {
                        op.operationState = MetaTypes.OperationStateType.EXECUTED;
                    }
                    oc.level = oc.level + 1;
                    MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
                } else {
                    context.wait_op = op;
                    break;
                }
            } else {
                if (op.operationState.equals(MetaTypes.OperationStateType.BLOCKED)) {
                    op.pdCount.decrementAndGet();
                    op.operationState = MetaTypes.OperationStateType.READY;
                    oc.level = oc.level + 1;
                }
            }
        }
    }

    @Override
    public void EXPLORE(Context context) {
        context.next();
    }

    @Override
    public boolean FINISHED(Context context) {
        return context.isFinished;
    }

    @Override
    public void RESET(Context context) {
        context.needCheckId = true;
        context.isFinished = false;
        for (OperationChain oc : this.tpg.threadToOCs.get(context.thisThreadId)) {
            oc.level = 0;
            oc.operations.clear();
        }
        this.abortHandling.compareAndSet(true, false);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
    }

    private void checkAbort() {
        this.abortHandling.compareAndSet(false, true);
    }

    private void REINITIALIZE(Context context) {
        context.isFinished = false;
        for (OperationChain oc : this.tpg.threadToOCs.get(context.thisThreadId)) {
            oc.level = 0;
            resetOp(oc);
        }
        context.scheduledTasks = 0;
        context.totalTasks = 0;
        INITIALIZE(context);
    }

    private void resetOp(OperationChain oc) {
        for (Operation op : oc.operations) {
            if (op.operationState.equals(MetaTypes.OperationStateType.ABORTED)) {
                oc.level = oc.level + 1;
            } else {
                op.operationState = MetaTypes.OperationStateType.BLOCKED;
            }
        }
    }

    @Override
    public void start_evaluation(Context context, long mark_ID, int num_events, int batchID) {
        INITIALIZE(context);
        do {
            EXPLORE(context);
            PROCESS(context, mark_ID, batchID);
        } while (!FINISHED(context));
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        if (this.abortHandling.get()) {
            LOG.info("Abort handling " + context.thisThreadId);
            REINITIALIZE(context);
            do {
                EXPLORE(context);
                PROCESS(context, mark_ID, batchID);
            } while (!FINISHED(context));
        }
        RESET(context);
    }

    public Context getTargetContext(String key) {
        int threadId = Integer.parseInt(key) / delta;
        return tpg.threadToContextMap.get(threadId);
    }

    private void inspectDependency(long groupId, OperationChain curOC, Operation op, String table_name,
                                   String key, String[] condition_sourceTable, String[] condition_source) {
        if (Objects.equals(appName, "GrepSum")) {
            inspectGSDependency(groupId, curOC, op, table_name, key, condition_sourceTable, condition_source);
        } else if (Objects.equals(appName, "StreamLedger")) {
            inspectSLDependency(groupId, curOC, op, table_name, key, condition_sourceTable, condition_source);
        }
    }

    private void graphConstruct(Context context) {
        for (OperationChain oc : tpg.threadToOCs.get(context.thisThreadId)) {
            this.tpg.threadToPathRecord.get(context.thisThreadId).addNode(oc.getTableName(), oc.getPrimaryKey(), oc.operations.size());
            // IOUtils.println("Thread " + context.thisThreadId + " has " + oc.operations.size() + " operations in " + oc.getTableName() + " " + oc.getPrimaryKey());
        }
    }

    public void execute(Operation operation, long mark_ID, boolean clean) {
//        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
//            Transfer_Fun(operation, mark_ID, clean);
//            if (operation.stateAccess.getStateObject(defaultString) != null) {
//                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
//            }
//        } else if (operation.accessType.equals(READ_WRITE_COND)) {
//            if (Objects.equals(appName, "StreamLedger")) {//SL
//                Transfer_Fun(operation, mark_ID, clean);
//            }
//        } else if (operation.accessType.equals(READ_WRITE)) {
//            if (Objects.equals(appName, "StreamLedger")) {
//                Depo_Fun(operation, mark_ID, clean);
//            }
//        } else if (operation.accessType.equals(READ_WRITE_COND_READN)) {
//            GrepSum_Fun(operation, mark_ID, clean);
//            if (operation.stateAccess.getStateObject(defaultString) != null) {
//                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
//            }
//        } else if (operation.accessType.equals(READ_WRITE_READ)) {
//            TollProcess_Fun(operation, mark_ID, clean);
//        }
    }

//    protected void Transfer_Fun(AbstractOperation operation, long previous_mark_ID, boolean clean) {
//        Operation op = (Operation) operation;
//        final long sourceAccountBalance;
//        if (op.historyView == null) {
//            SchemaRecord preValues = operation.condition_records.get(defaultString).content_.readPreValues(operation.bid);
//            sourceAccountBalance = preValues.getValues().get(1).getLong();
//        } else {
//            sourceAccountBalance = Long.parseLong(String.valueOf(op.historyView));
//        }
//        // apply function
//        AppConfig.randomDelay();
//
//        if (sourceAccountBalance > 100) {//Old conditions: event.getMinAccountBalance()(default=0), event.getAccountTransfer()(default=100)
//            // read
//            SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
//            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
//
//            if (operation.stateAccess.getValue("function") == "INC") {
//                tempo_record.getValues().get(1).incLong(sourceAccountBalance, (Long) operation.stateAccess.getValue("delta_long"));//compute.
//            } else if (operation.stateAccess.getValue("function") == "DEC") {
//                tempo_record.getValues().get(1).decLong(sourceAccountBalance, (Long) operation.stateAccess.getValue("delta_long"));//compute.
//            } else
//                throw new UnsupportedOperationException();
//            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
//        } else {
//            op.isFailed.set(true);
//        }
//    }
//
//    protected void Depo_Fun(AbstractOperation operation, long mark_ID, boolean clean) {
//        SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
//        List<DataBox> values = srcRecord.getValues();
//        //apply function to modify..
//        AppConfig.randomDelay();
//        SchemaRecord tempo_record;
//        tempo_record = new SchemaRecord(values);//tempo record
//        tempo_record.getValues().get(1).incLong((Long) operation.stateAccess.getValue("delta_long"));//compute.
//        operation.d_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
//    }
//
//    protected void GrepSum_Fun(Operation operation, long previous_mark_ID, boolean clean) {
//        int keysLength = operation.condition_records.size();
//        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.size()];
//        long sum = 0;
//        AppConfig.randomDelay();
//        if (operation.historyView != null) {
//            sum = Long.parseLong(String.valueOf(operation.historyView));
//        } else {
//            int i = 0;
//            for (TableRecord tableRecord : operation.condition_records.values()) {
//                preValues[i] = tableRecord.content_.readPreValues(operation.bid);
//                sum += preValues[i].getValues().get(1).getLong();
//                i++;
//            }
//        }
//        sum /= keysLength;
//        SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
//        SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
//        if ((Long) operation.stateAccess.getValue("delta_long") != -1) {
//            if (operation.stateAccess.getValue("function") == "SUM") {
//                tempo_record.getValues().get(1).setLong(sum);//compute.
//            } else
//                throw new UnsupportedOperationException();
//            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
//        } else {
//            operation.isFailed.set(true);
//        }
//
//    }
//
//    protected void TollProcess_Fun(Operation operation, long previous_mark_ID, boolean clean) {
//        AppConfig.randomDelay();
//        List<DataBox> srcRecord = operation.d_record.record_.getValues();
//        if (operation.stateAccess.getValue("function") == "AVG") {
//            if ((double) operation.stateAccess.getValue("delta_double") < MAX_SPEED) {
//                double latestAvgSpeeds = srcRecord.get(1).getDouble();
//                double lav;
//                if (latestAvgSpeeds == 0) {//not initialized
//                    lav = (double) operation.stateAccess.getValue("delta_double");
//                } else
//                    lav = (latestAvgSpeeds + (double) operation.stateAccess.getValue("delta_double")) / 2;
//
//                srcRecord.get(1).setDouble(lav);//write to state.
//                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
//            } else {
//                operation.isFailed.set(true);
//            }
//        } else {
//            if ((int) operation.stateAccess.getValue("delta_int") < MAX_INT) {
//                HashSet cnt_segment = srcRecord.get(1).getHashSet();
//                cnt_segment.add(operation.stateAccess.getValue("delta_int"));//update hashset; updated state also. TODO: be careful of this.
//                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
//            } else {
//                operation.isFailed.set(true);
//            }
//        }
//    }

    private void inspectSLDependency(long groupId, OperationChain curOC, Operation op, String table_name,
                                     String key, String[] condition_sourceTable, String[] condition_source) {
        if (condition_source != null) {
            for (int index = 0; index < condition_source.length; index++) {
                if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                    continue;
                Object history = null;
                if (FaultToleranceRelax.isHistoryView) {
                    history = this.loggingManager.inspectDependencyView(groupId, table_name, key, condition_source[index], op.bid);
                }
                if (history != null) {
                    op.historyView = history;
                } else {
                    if (FaultToleranceRelax.isAbortPushDown) {
                        OperationChain OCFromConditionSource = tpg.getOC(condition_sourceTable[index], condition_source[index]);
                        //Add dependent Oc
                        op.incrementPd(OCFromConditionSource);
                        //Add the shadow operations
                        OCFromConditionSource.addOperation(op);
                    }
                }
            }
        }
    }

    private void inspectGSDependency(long groupId, OperationChain curOC, Operation op, String table_name,
                                     String key, String[] condition_sourceTable, String[] condition_source) {
        if (condition_source != null) {
            for (int index = 0; index < condition_source.length; index++) {
                if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                    continue;
                Object history = null;
                if (FaultToleranceRelax.isHistoryView) {
                    history = this.loggingManager.inspectDependencyView(groupId, table_name, key, condition_source[index], op.bid);
                }
                if (history != null) {
                    op.historyView = history;
                } else {
                    if (FaultToleranceRelax.isAbortPushDown) {
                        OperationChain OCFromConditionSource = tpg.getOC(condition_sourceTable[index], condition_source[index]);
                        //Add dependent Oc
                        op.incrementPd(OCFromConditionSource);
                        //Add the shadow operations
                        OCFromConditionSource.addOperation(op);
                    }
                }
            }
        }
    }
}
