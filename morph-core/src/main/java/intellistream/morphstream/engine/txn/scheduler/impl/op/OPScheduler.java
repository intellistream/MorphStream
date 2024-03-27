package intellistream.morphstream.engine.txn.scheduler.impl.op;


import intellistream.morphstream.api.Client;
import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.durability.logging.LoggingEntry.LogRecord;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.CommandLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.DependencyLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.LSNVectorLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager.PathLoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.durability.struct.Logging.DependencyLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LVCLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.NativeCommandLog;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.scheduler.Request;
import intellistream.morphstream.engine.txn.scheduler.context.op.OPSchedulerContext;
import intellistream.morphstream.engine.txn.scheduler.impl.IScheduler;
import intellistream.morphstream.engine.txn.scheduler.struct.op.MetaTypes;
import intellistream.morphstream.engine.txn.scheduler.struct.op.Operation;
import intellistream.morphstream.engine.txn.scheduler.struct.op.TaskPrecedenceGraph;
import intellistream.morphstream.engine.txn.scheduler.struct.op.WindowDescriptor;
import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.AppConfig;
import communication.dao.TPGEdge;
import communication.dao.TPGNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

import static intellistream.morphstream.configuration.CONTROL.enable_latency_measurement;
import static intellistream.morphstream.util.FaultToleranceConstants.*;

public abstract class OPScheduler<Context extends OPSchedulerContext, Task> implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPScheduler.class);
    public final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph<Context> tpg; // TPG to be maintained in this global instance.
    public LoggingManager loggingManager; // Used by fault tolerance
    public int isLogging;// Used by fault tolerance
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
    private MetaTypes.DependencyType[] dependencyTypes = new MetaTypes.DependencyType[]{MetaTypes.DependencyType.FD, MetaTypes.DependencyType.TD, MetaTypes.DependencyType.LD};

    public OPScheduler(int totalThreads, int NUM_ITEMS) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads); // Check id generation in DateGenerator.
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, delta, NUM_ITEMS);
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

    public Context getTargetContext(String key) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = Integer.parseInt(key) / delta;
        return tpg.threadToContextMap.get(threadId);
    }

    public Context getTargetContext(TableRecord d_record) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId = getTaskId(d_record.record_.GetPrimaryKey(), delta);
        return tpg.threadToContextMap.get(threadId);
    }

    /**
     * Used by tpgScheduler.
     *
     * @param operation
     * @param mark_ID
     * @param clean
     * @param batchID
     */
    public void execute(Operation operation, long mark_ID, boolean clean, int batchID) {
//        log.trace("++++++execute: " + operation);
        // if the operation is in state aborted or committable or committed, we can bypass the execution
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED) || operation.isFailed.get()) {
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

        if (enable_latency_measurement) {
            String operationID = operation.stateAccess.getOperationID();
            TPGNode node = new TPGNode(operationID, operation.accessType.toString(), operation.table_name, operation.d_record.record_.GetPrimaryKey());
            for (MetaTypes.DependencyType type : dependencyTypes) {
                for (Operation child : operation.getChildren(type)) {
                    TPGEdge edge = new TPGEdge(operationID, child.stateAccess.getOperationID(), type.toString());
                }
            }
        }

        // apply function
        AppConfig.randomDelay();

        /**
         * Start of newly defined txn execution logic
         */
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
                //TODO: pass in the write object-type, avoid isInstanceOf check
                tempo_record.getValues().get(1).setDouble((double) udfResult);
                operation.d_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);
                //Assign updated schemaRecord back to stateAccess
                operation.stateAccess.setUpdatedStateObject(tempo_record);
            }
        } else {
            operation.stateAccess.setAborted();
            operation.isFailed.set(true);
        }
        /**
         * End of newly defined txn execution logic
        */

        commitLog(operation);
        assert operation.getOperationState() != MetaTypes.OperationStateType.EXECUTED;
    }

    @Override
    public void AddContext(int threadId, Context context) {
        tpg.threadToContextMap.put(threadId, context);
        /*Thread to OCs does not need reconfigure*/
        tpg.setOCs(context);
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

    protected abstract void DISTRIBUTE(Task task, Context context);

    @Override
    public void RESET(Context context) {
        //SOURCE_CONTROL.getInstance().oneThreadCompleted(context.thisThreadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.thisThreadId);
        context.reset();
        tpg.reset(context);
    }

    @Override
    public void TxnSubmitFinished(Context context, int batchID) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        // the data structure to store all operations created from the txn, store them in order, which indicates the logical dependency
        int txnOpId = 0;
        Operation headerOperation = null;
        for (Request request : context.requests) {
            //all requests under the same request share LD
            long bid = request.txn_context.getBID();
            Operation set_op;
            switch (request.accessType) {
                case WRITE_ONLY:
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, null, request.stateAccess);
                    break;
                case READ:
                case WRITE:
                case READ_WRITE: // they can use the same method for processing
                case READ_WRITE_COND:
                case READ_WRITE_COND_READ:
                case READ_WRITE_COND_READN:
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, request.stateAccess);
                    break;
                case READ_WRITE_READ:
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, null, request.stateAccess);
                    break;
                case NON_DETER_READ:
                case NON_DETER_WRITE:
                case NON_READ_WRITE_COND_READN:
                    set_op = new Operation(true, request.tables, request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, request.stateAccess);
                    break;
                case WINDOW_READ:
                case WINDOW_WRITE:
                case WINDOWED_READ_ONLY:
                    WindowDescriptor windowContext = new WindowDescriptor(true, AppConfig.windowSize);
                    set_op = new Operation(request.write_key, getTargetContext(request.write_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, windowContext, request.stateAccess);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
//            set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
            tpg.setupOperationTDFD(set_op, request);
            if (txnOpId == 0) {
                headerOperation = set_op; //In TPG, this is the LD parent for all operations in the same txn
            }

            // Update LD
            String operationID = set_op.stateAccess.getOperationID();
            String LDParentOperationID = String.valueOf(headerOperation.stateAccess.getOperationID());
            TPGNode node = new TPGNode(operationID, set_op.accessType.toString(), set_op.table_name, set_op.d_record.record_.GetPrimaryKey());
            TPGEdge edge = new TPGEdge(LDParentOperationID, operationID, MetaTypes.DependencyType.LD.toString());

            // addOperation an operation id for the operation for the purpose of temporal dependency construction
            set_op.setTxnOpId(txnOpId++);
            set_op.addHeader(headerOperation);
            headerOperation.addDescendant(set_op);
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    protected abstract void NOTIFY(Operation operation, Context context);

    public void start_evaluation(Context context, long mark_ID, int num_events, int batchID) {
        int threadId = context.thisThreadId;

        INITIALIZE(context);

        do {
//            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
            EXPLORE(context);
//            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            PROCESS(context, mark_ID, batchID);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
//            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(threadId);
        } while (!FINISHED(context));
        RESET(context);//
//        MeasureTools.SCHEDULE_TIME_RECORD(threadId, num_events);
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
        if (operation.isCommit)
            return;
        if (isLogging == LOGOption_path) {
            return;
        }
        MeasureTools.BEGIN_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
        if (isLogging == LOGOption_wal) {
            ((LogRecord) operation.logRecord).addUpdate(operation.d_record.content_.readPreValues(operation.bid));
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_dependency) {
            ((DependencyLog) operation.logRecord).setId(operation.bid + "." + operation.txnOpId);
            for (Operation op : operation.fd_parents) {
                ((DependencyLog) operation.logRecord).addInEdge(op.bid + "." + op.txnOpId);
            }
            for (Operation op : operation.td_parents) {
                ((DependencyLog) operation.logRecord).addInEdge(op.bid + "." + op.txnOpId);
            }
            for (Operation op : operation.fd_children) {
                ((DependencyLog) operation.logRecord).addOutEdge(op.bid + "." + op.txnOpId);
            }
            for (Operation op : operation.td_children) {
                ((DependencyLog) operation.logRecord).addOutEdge(op.bid + "." + op.txnOpId);
            }
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_lv) {
            ((LVCLog) operation.logRecord).setAccessType(operation.accessType);
            ((LVCLog) operation.logRecord).setThreadId(operation.context.thisThreadId);
            this.loggingManager.addLogRecord(operation.logRecord);
        } else if (isLogging == LOGOption_command) {
            ((NativeCommandLog) operation.logRecord).setId(operation.bid + "." + operation.txnOpId);
            this.loggingManager.addLogRecord(operation.logRecord);
        }
        operation.isCommit = true;
        MeasureTools.END_SCHEDULE_TRACKING_TIME_MEASURE(operation.context.thisThreadId);
    }

}
