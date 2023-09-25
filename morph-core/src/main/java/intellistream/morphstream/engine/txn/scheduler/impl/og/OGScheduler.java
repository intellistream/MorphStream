package intellistream.morphstream.engine.txn.scheduler.impl.og;


import intellistream.morphstream.engine.txn.durability.logging.LoggingEntry.LogRecord;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.CommandLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.DependencyLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.LSNVectorLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.PathLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.durability.struct.Logging.DependencyLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.HistoryLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LVCLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.NativeCommandLog;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.og.OGSchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.og.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.og.OperationChain;
import intellistream.morphstream.engine.txn.scheduler.struct.og.TaskPrecedenceGraph;
import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.op.WindowDescriptor;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.datatype.DoubleDataBox;
import intellistream.morphstream.engine.txn.storage.datatype.IntDataBox;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.MyList;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.*;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.defaultString;
import static intellistream.morphstream.util.FaultToleranceConstants.*;

public abstract class OGScheduler<Context extends OGSchedulerContext>
        implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OGScheduler.class);
    public final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph<Context> tpg; // TPG to be maintained in this global instance.
    public LoggingManager loggingManager; // Used by fault tolerance
    public int isLogging;// Used by fault tolerance

    protected OGScheduler(int totalThreads, int NUM_ITEMS, int app) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads); // Check id generation in DateGenerator.
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, delta, NUM_ITEMS, app);
    }

    /**
     * state to thread mapping
     *
     * @param key
     * @param delta
     * @return
     */
    public static int getTaskId(String key, Integer delta) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }

    @Override
    public void initTPG(int offset) {
        tpg.initTPG(offset);
    }

    public Context getTargetContext(TableRecord d_record) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = getTaskId(d_record.record_.GetPrimaryKey(), delta);
        return tpg.threadToContextMap.get(threadId);
    }

    public Context getTargetContext(String key) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = Integer.parseInt(key) / delta;
        return tpg.threadToContextMap.get(threadId);
    }


    public void start_evaluation(Context context, long mark_ID, int num_events) {
        int threadId = context.thisThreadId;
        INITIALIZE(context);

        do {
//            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            EXPLORE(context);
//            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            PROCESS(context, mark_ID);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
//            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
        } while (!FINISHED(context));
        RESET(context);//

//        MeasureTools.SCHEDULE_TIME_RECORD(threadId, num_events);
    }

    /**
     * Transfer event processing
     *
     * @param operation
     * @param previous_mark_ID
     * @param clean
     */
    protected void Transfer_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        SchemaRecord preValues = operation.condition_records.get("default_key").content_.readPreValues(operation.bid);
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();

        // apply function
        AppConfig.randomDelay();

        if (sourceAccountBalance > 100) {//Old conditions: event.getMinAccountBalance()(default=0), event.getAccountTransfer()(default=100)
            // read
            SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (operation.stateAccess.getValue("function") == "INC") {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, (Long) operation.stateAccess.getValue("delta_long"));//compute.
            } else if (operation.stateAccess.getValue("function") == "DEC") {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, (Long) operation.stateAccess.getValue("delta_long"));//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        } else {
            operation.isFailed.set(true);
        }
        if (!operation.isFailed.get()) {
            if (isLogging == LOGOption_path && !operation.pKey.equals(preValues.GetPrimaryKey()) && !operation.isCommit) {
                MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
                int id = getTaskId(operation.pKey, delta);
                this.loggingManager.addLogRecord(new HistoryLog(id, operation.table_name, operation.pKey, preValues.GetPrimaryKey(), operation.bid, sourceAccountBalance));
                operation.isCommit = true;
                MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
            }
        }
    }

    /**
     * Deposite event processing
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    protected void Depo_Fun(Operation operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        AppConfig.randomDelay();
        //apply function to modify..
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong((Long) operation.stateAccess.getValue("delta_long"));//compute.
        operation.d_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    protected void GrepSum_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        int keysLength = operation.condition_records.size();
        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.size()];

        long sum = 0;

        // apply function
        AppConfig.randomDelay();

        int i = 0;
        for (TableRecord tableRecord : operation.condition_records.values()) {
            preValues[i] = tableRecord.content_.readPreValues(operation.bid);
            sum += preValues[i].getValues().get(1).getLong();
            i++;
        }

        sum /= keysLength;

        if ((Long) operation.stateAccess.getValue("delta_long") != -1) {
            // read
            SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            // apply function

            if (operation.stateAccess.getValue("function") == "SUM") {
//                tempo_record.getValues().get(1).incLong(tempo_record, sum);//compute.
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        } else {
            operation.isFailed.set(true);
        }
    }

    protected void Non_GrepSum_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        int keysLength = operation.condition_records.size();
        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.size()];

        long sum = 0;
        // apply function
        AppConfig.randomDelay();

        int i = 0;
        for (TableRecord tableRecord : operation.condition_records.values()) {
            //Get Deterministic Key
            preValues[i] = tableRecord.content_.readPreValues(operation.bid);
            long value = preValues[i].getValues().get(1).getLong();
            //Read the corresponding value
            preValues[i] = operation.tables[i].SelectKeyRecord(String.valueOf(value)).content_.readPreValues(operation.bid);
            i++;
        }

        sum /= keysLength;

        if ((Long) operation.stateAccess.getValue("delta_long") != -1) {
            // Get Deterministic Key
            SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
            long srcValue = srcRecord.getValues().get(1).getLong();
            // Read the corresponding value
            SchemaRecord deterministicSchemaRecord = operation.tables[0].SelectKeyRecord(String.valueOf(srcValue)).content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(deterministicSchemaRecord);//tempo record
            if (operation.stateAccess.getValue("function") == "SUM") {
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            // Get Deterministic Key
            SchemaRecord disRecord = operation.d_record.content_.readPreValues(operation.bid);
            long disValue = srcRecord.getValues().get(1).getLong();
            // Read the corresponding value
            TableRecord disDeterministicRecord = operation.tables[0].SelectKeyRecord(String.valueOf(disValue));
            disDeterministicRecord.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            operation.deterministicRecords = new TableRecord[1];
            operation.deterministicRecords[0] = disDeterministicRecord;
        } else {
            operation.isFailed.set(true);
        }
    }

    protected void Windowed_GrepSum_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        int keysLength = operation.condition_records.size();

        long sum = 0;

        // apply function
        AppConfig.randomDelay();

        for (TableRecord tableRecord : operation.condition_records.values()) {
            assert operation.windowContext.isWindowed();
            List<SchemaRecord> schemaRecordRange = tableRecord.content_.readPreValuesRange(operation.bid, operation.windowContext.getRange());
            sum += schemaRecordRange.stream().mapToLong(schemaRecord -> schemaRecord.getValues().get(1).getLong()).sum();
        }

        sum /= keysLength;

        if ((Long) operation.stateAccess.getValue("delta_long") != -1) { // TODO: we use the d_record to store the aggregated result here, will be optimized in the future.
            // read
            SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (operation.stateAccess.getValue("function") == "SUM") {
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        } else {
            operation.isFailed.set(true);
        }
    }

    /**
     * general operation execution entry method for all schedulers.
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    public void execute(Operation operation, long mark_ID, boolean clean) {
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            //otherwise, skip (those +already been tagged as aborted).
            if (operation.isNonDeterministicOperation && operation.deterministicRecords != null) {
                for (TableRecord tableRecord : operation.deterministicRecords) {
                    tableRecord.content_.DeleteLWM(operation.bid);
                }
            } else {
                operation.d_record.content_.DeleteLWM(operation.bid);
            }
            commitLog(operation);
            return;
        }
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            Transfer_Fun(operation, mark_ID, clean);
            if (operation.stateAccess.getStateObject(defaultString) != null) {
                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            if (this.tpg.getApp() == 1) {//SL
                Transfer_Fun(operation, mark_ID, clean);
            } else {//OB
                AppConfig.randomDelay();
                List<DataBox> d_record = operation.condition_records.get(defaultString).content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType).getValues();
                long askPrice = d_record.get(1).getLong();//price
                long left_qty = d_record.get(2).getLong();//available qty;
                long bidPrice = 100; //old condition: event.getBidPrice(i), default=100
                long bid_qty = 1; //old condition: event.getBidQty(i)), default=1
                if (bidPrice > askPrice || bid_qty < left_qty) {
                    d_record.get(2).setLong(left_qty - (Long) operation.stateAccess.getValue("delta_long"));//new quantity.
                } else {
                    operation.isFailed.set(true);
                }
            }
        } else if (operation.accessType.equals(READ_WRITE)) {
            if (this.tpg.getApp() == 1) { //SL
                Depo_Fun(operation, mark_ID, clean);
            } else {
                AppConfig.randomDelay();
                SchemaRecord srcRecord = operation.d_record.content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType);
                List<DataBox> values = srcRecord.getValues();
                if (operation.stateAccess.getValue("function") == "INC") {
                    values.get(2).setLong(values.get(2).getLong() + (Long) operation.stateAccess.getValue("delta_long"));
                } else
                    throw new UnsupportedOperationException();
            }
        } else if (operation.accessType.equals(READ_WRITE_COND_READN)) {
            GrepSum_Fun(operation, mark_ID, clean);
            if (operation.stateAccess.getStateObject(defaultString) != null) {
                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
        } else if (operation.accessType.equals(NON_READ_WRITE_COND_READN)) {
            Non_GrepSum_Fun(operation, mark_ID, clean);
            if (operation.stateAccess.getStateObject(defaultString) != null) {
                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
        } else if (operation.accessType.equals(READ_WRITE_READ)) {
            //TODO: implement the operation
            assert operation.stateAccess.getStateObject(defaultString) != null;
            AppConfig.randomDelay();
            List<DataBox> srcRecord = operation.d_record.record_.getValues();
            if (operation.stateAccess.getValue("function") == "AVG") {
                if (true) { //TODO: Original condition: operation.condition.arg1 < operation.condition.arg2
                    double latestAvgSpeeds = srcRecord.get(1).getDouble();
                    double lav;
                    if (latestAvgSpeeds == 0) {//not initialized
                        lav = (double) operation.stateAccess.getValue("delta_double");
                    } else
                        lav = (latestAvgSpeeds + (double) operation.stateAccess.getValue("delta_double")) / 2;

                    srcRecord.get(1).setDouble(lav);//write to state.
                    operation.stateAccess.getStateObject(defaultString).setSchemaRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
                } else {
                    operation.isFailed.set(true);
                }
            } else {
                HashSet cnt_segment = srcRecord.get(1).getHashSet();
                cnt_segment.add(operation.stateAccess.getValue("delta_int"));//update hashset; updated state also. TODO: be careful of this.
                operation.stateAccess.getStateObject(defaultString).setSchemaRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
            }
        } else if (operation.accessType.equals(WRITE_ONLY)) {
            //OB-Alert
            AppConfig.randomDelay();
            operation.d_record.record_.getValues().get(1).setLong((Long) operation.stateAccess.getValue("ask_price")); //TODO: Refine it, used to be OBEvent.askPrice
        } else if (operation.accessType.equals(WINDOWED_READ_ONLY)) {
            assert operation.stateAccess.getStateObject(defaultString) != null;
            AppConfig.randomDelay();
            Windowed_GrepSum_Fun(operation, mark_ID, clean);
            operation.stateAccess.getStateObject(defaultString).setSchemaRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
        } else {
            throw new UnsupportedOperationException();
        }
        commitLog(operation);
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
        OperationChain next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
//            assert !next.getOperations().isEmpty();
            if (executeWithBusyWait(context, next, mark_ID)) { // only when executed, the notification will start.
                MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
                NOTIFY(next, context);
                MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
            }
        } else {
//            if (AppConfig.isCyclic) {
            MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(context.thisThreadId);
            next = nextFromBusyWaitQueue(context);
            MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);
            if (next != null) {
//                assert !next.getOperations().isEmpty();
                if (executeWithBusyWait(context, next, mark_ID)) { // only when executed, the notification will start.
                    MeasureTools.BEGIN_NOTIFY_TIME_MEASURE(threadId);
                    NOTIFY(next, context);
                    MeasureTools.END_NOTIFY_TIME_MEASURE(threadId);
                }
            }
//            }
        }
    }

    /**
     * Try to get task from local queue.
     *
     * @param context
     * @return
     */
    protected OperationChain next(Context context) {
        throw new UnsupportedOperationException();
    }

    public boolean executeWithBusyWait(Context context, OperationChain operationChain, long mark_ID) {
        MyList<Operation> operation_chain_list = operationChain.getOperations();
        for (Operation operation : operation_chain_list) {
            if (operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)
                    || operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)
                    || operation.isFailed.get()) continue;
            if (isConflicted(context, operationChain, operation)) {
                return false;
            }
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            if (!operation.isFailed.get() && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
                operation.stateTransition(MetaTypes.OperationStateType.EXECUTED);
            } else {
                checkTransactionAbort(operation, operationChain);
            }
        }
        return true;
    }

    protected void checkTransactionAbort(Operation operation, OperationChain operationChain) {
        // in coarse-grained algorithms, we will not handle transaction abort gracefully, just update the state of the operation
        operation.stateTransition(MetaTypes.OperationStateType.ABORTED);
        // save the abort information and redo the batch.
    }

    protected OperationChain nextFromBusyWaitQueue(Context context) {
        return context.busyWaitQueue.poll();
    }

    protected abstract void DISTRIBUTE(OperationChain task, Context context);

    protected abstract void NOTIFY(OperationChain task, Context context);

    @Override
    public boolean FINISHED(Context context) {
        return context.finished();
    }

    /**
     * Submit requests to target thread --> data shuffling is involved.
     *
     * @param context
     * @param request
     * @return
     */
    @Override
    public boolean SubmitRequest(Context context, Request request) {
        context.push(request);
        return false;
    }

    @Override
    public void RESET(Context context) {
//        SOURCE_CONTROL.getInstance().oneThreadCompleted();
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
//        SOURCE_CONTROL.getInstance().waitForOtherThreadsAbort();
        context.reset();
        tpg.reset(context);
    }

    @Override
    public void TxnSubmitBegin(Context context) {
        context.requests.clear();
    }

    @Override
    public void TxnSubmitFinished(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        List<Operation> operationGraph = new ArrayList<>();
        int txnOpId = 0;
        Operation headerOperation = null;
        Operation set_op;
        for (Request request : context.requests) {
            set_op = constructOp(operationGraph, request);
            if (txnOpId == 0)
                headerOperation = set_op;
            // addOperation an operation id for the operation for the purpose of temporal dependency construction
            set_op.setTxnOpId(txnOpId++);
            set_op.addHeader(headerOperation);
            headerOperation.addDescendant(set_op);
        }
        // set logical dependencies among all operation in the same transaction
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    private Operation constructOp(List<Operation> operationGraph, Request request) {
        long bid = request.txn_context.getBID();
        Operation set_op;
        Context targetContext = getTargetContext(request.write_key);
        switch (request.accessType) {
            case WRITE_ONLY:
                set_op = new Operation(false, null, request.write_key, request.table_name, null,
                        request.txn_context, request.accessType, request.d_record, bid, targetContext, null, request.stateAccess);
                break;
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new Operation(false, null, request.write_key, request.table_name, request.condition_records,
                        request.txn_context, request.accessType, request.d_record, bid, targetContext, null, request.stateAccess);
                break;
            case READ_WRITE_COND_READ:
            case READ_WRITE_COND_READN:
                set_op = new Operation(false, null, request.write_key, request.table_name, request.condition_records,
                        request.txn_context, request.accessType, request.d_record, bid, targetContext, null, request.stateAccess);
                break;
            case NON_READ_WRITE_COND_READN:
                set_op = new Operation(true, request.tables, request.write_key, request.table_name, request.condition_records,
                        request.txn_context, request.accessType, request.d_record, bid, targetContext, null, request.stateAccess);
                break;
            case READ_WRITE_READ:
                set_op = new Operation(false, null, request.write_key, request.table_name, null,
                        request.txn_context, request.accessType, request.d_record, bid, targetContext, null, request.stateAccess);
                break;
            case WINDOWED_READ_ONLY:
                WindowDescriptor windowContext = new WindowDescriptor(true, AppConfig.windowSize);
                set_op = new Operation(false, null, request.write_key, request.table_name, request.condition_records,
                        request.txn_context, request.accessType, request.d_record, bid, targetContext, windowContext, request.stateAccess);
                break;
            default:
                throw new RuntimeException("Unexpected operation");
        }
        operationGraph.add(set_op);
        tpg.setupOperationTDFD(set_op, request, targetContext);
        return set_op;
    }

    @Override
    public void AddContext(int threadId, Context context) {
        tpg.threadToContextMap.put(threadId, context);
        tpg.setOCs(context);
    }

    protected boolean isConflicted(Context context, OperationChain operationChain, Operation operation) {
        if (operation.fd_parents != null) {
            for (Operation conditioned_operation : operation.fd_parents) {
                if (conditioned_operation != null) {
                    if (!(conditioned_operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)
                            || conditioned_operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)
                            || conditioned_operation.isFailed.get())) {
                        // blocked and busy wait
                        context.busyWaitQueue.add(operationChain);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public void setLoggingManager(LoggingManager loggingManager) {
        this.loggingManager = loggingManager;
        if (loggingManager instanceof PathLoggingManager) {
            isLogging = LOGOption_path;
            this.tpg.threadToPathRecord = ((PathLoggingManager) loggingManager).threadToPathRecord;
        } else if (loggingManager instanceof LSNVectorLoggingManager) {
            isLogging = LOGOption_lv;
        } else if (loggingManager instanceof DependencyLoggingManager) {
            isLogging = LOGOption_dependency;
        } else if (loggingManager instanceof CommandLoggingManager) {
            isLogging = LOGOption_command;
        } else {
            isLogging = LOGOption_no;
        }
        tpg.isLogging = isLogging;
    }

    private void commitLog(Operation operation) {
        if (operation.isCommit) {
            return;
        }
        if (isLogging == LOGOption_path) {
            return;
        }
        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
        if (isLogging == LOGOption_wal) {
            ((LogRecord) operation.logRecord).addUpdate(operation.d_record.content_.readPreValues(operation.bid));
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_dependency) {
            ((DependencyLog) operation.logRecord).setId(operation.bid + "." + operation.getTxnOpId());
            for (Operation op : operation.fd_parents) {
                ((DependencyLog) operation.logRecord).addInEdge(op.bid + "." + op.getTxnOpId());
            }
            for (Operation op : operation.fd_children) {
                ((DependencyLog) operation.logRecord).addOutEdge(op.bid + "." + op.getTxnOpId());
            }
            Operation ldParent = operation.getOC().getOperations().lower(operation);
            if (ldParent != null)
                ((DependencyLog) operation.logRecord).addInEdge(ldParent.bid + "." + ldParent.getTxnOpId());
            Operation ldChild = operation.getOC().getOperations().higher(operation);
            if (ldChild != null)
                ((DependencyLog) operation.logRecord).addOutEdge(ldChild.bid + "." + ldChild.getTxnOpId());
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_lv) {
            ((LVCLog) operation.logRecord).setAccessType(operation.accessType);
            ((LVCLog) operation.logRecord).setThreadId(operation.context.thisThreadId);
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_command) {
            ((NativeCommandLog) operation.logRecord).setId(operation.bid + "." + operation.getTxnOpId());
            this.loggingManager.addLogRecord(operation.logRecord);
        }
        operation.isCommit = true;
        MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
    }
}