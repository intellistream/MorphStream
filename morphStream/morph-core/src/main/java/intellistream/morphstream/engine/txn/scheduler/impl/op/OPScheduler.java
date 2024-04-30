package intellistream.morphstream.engine.txn.scheduler.impl.op;


import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.state.StateAccess;
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
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.AppConfig;
import intellistream.morphstream.util.libVNFFrontend.NativeInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

import static intellistream.morphstream.util.FaultToleranceConstants.*;

public abstract class OPScheduler<Context extends OPSchedulerContext, Task> implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPScheduler.class);
    public final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph<Context> tpg; // TPG to be maintained in this global instance.
    public LoggingManager loggingManager; // Used by fault tolerance
    public int isLogging;// Used by fault tolerance
    private native boolean nativeTxnUDF(String operatorID, StateAccess stateAccess);
    private final boolean useNativeLib = MorphStreamEnv.get().configuration().getBoolean("useNativeLib", false);
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);

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
     */
    public void execute(Operation operation, long mark_ID, boolean clean) {
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

        // set computation complexity
//        AppConfig.randomDelay();

        if (serveRemoteVNF) {
            //stateAccess: saID, type, writeObjIndex, table name, key's value, field index in table, access type
            int[] readValues = new int[operation.condition_records.size()]; //<stateObj field value> * N

            int saIndex = 0;
            for (TableRecord tableRecord : operation.condition_records) {
                int stateFieldIndex = Integer.parseInt(operation.stateAccess[3 + saIndex * 4 + 2]);
                SchemaRecord readRecord = tableRecord.content_.readPreValues(operation.bid);
                readValues[saIndex] = (int) readRecord.getValues().get(stateFieldIndex).getDouble();
                saIndex++;
            }

            ByteBuffer byteBuffer = ByteBuffer.allocate(readValues.length * 4);
            byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
            for (int value : readValues) {
                byteBuffer.putInt(value);
            }
            byte[] readBytes = byteBuffer.array();
            int isAbort = -1;
            int udfResult = -1;

            //TODO: Simplify saData into a single write value

            byte[] saResultBytes = NativeInterface._execute_sa_udf(operation.txnReqID, Integer.parseInt(operation.stateAccess[0]), readBytes, readValues.length);
            isAbort = decodeInt(saResultBytes, 0);
            udfResult = decodeInt(saResultBytes, 4);

            if (isAbort == 0) { //txn is not aborted
                if (operation.accessType == CommonMetaTypes.AccessType.WRITE
                        || operation.accessType == CommonMetaTypes.AccessType.WINDOW_WRITE
                        || operation.accessType == CommonMetaTypes.AccessType.NON_DETER_WRITE) {
                    //Update udf results to writeRecord
                    SchemaRecord srcRecord = operation.d_record.content_.readPreValues(operation.bid);
                    SchemaRecord tempo_record = new SchemaRecord(srcRecord);
                    tempo_record.getValues().get(1).setDouble(udfResult);
                    operation.d_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);
                }
            } else if (isAbort == 1) {
                operation.stateAccess[1] = "true"; //pass isAbort back to bolt
                operation.isFailed.set(true);
            } else {
                throw new RuntimeException();
            }

        } else {
            // Simplified saData: saID, saType, tableName, tupleID
            SchemaRecord simReadRecord = operation.d_record.content_.readPreValues(operation.bid);
            int simReadValue = simReadRecord.getValues().get(1).getInt();

            SchemaRecord simTempoRecord = new SchemaRecord(simReadRecord);
            simTempoRecord.getValues().get(1).setInt(simReadValue);
            operation.d_record.content_.updateMultiValues(operation.bid, mark_ID, clean, simTempoRecord);
        }

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
    public void TxnSubmitFinished(Context context) {
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
                    set_op = new Operation(request.d_key, getTargetContext(request.d_key), request.d_table, request.txn_context, bid, request.accessType,
                            request.d_record, null, request.stateAccess, request.d_fieldIndex, request.condition_fieldIndexes);
                    break;
                case READ:
                case WRITE:
                case READ_WRITE: // they can use the same method for processing
                case READ_WRITE_COND:
                case READ_WRITE_COND_READ:
                case READ_WRITE_COND_READN:
                    set_op = new Operation(request.d_key, getTargetContext(request.d_key), request.d_table, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, request.stateAccess, request.d_fieldIndex, request.condition_fieldIndexes);
                    break;
                case READ_WRITE_READ:
                    set_op = new Operation(request.d_key, getTargetContext(request.d_key), request.d_table, request.txn_context, bid, request.accessType,
                            request.d_record, null, request.stateAccess, request.d_fieldIndex, request.condition_fieldIndexes);
                    break;
                case NON_DETER_READ:
                case NON_DETER_WRITE:
                case NON_READ_WRITE_COND_READN:
                    set_op = new Operation(true, request.tables, request.d_key, getTargetContext(request.d_key), request.d_table, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, request.stateAccess, request.d_fieldIndex, request.condition_fieldIndexes);
                    break;
                case WINDOW_READ:
                case WINDOW_WRITE:
                case WINDOWED_READ_ONLY:
                    WindowDescriptor windowContext = new WindowDescriptor(true, AppConfig.windowSize);
                    set_op = new Operation(request.d_key, getTargetContext(request.d_key), request.d_table, request.txn_context, bid, request.accessType,
                            request.d_record, request.condition_records, windowContext, request.stateAccess, request.d_fieldIndex, request.condition_fieldIndexes);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
            set_op.txnReqID = request.txn_context.getTxnReqID();
//            set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
            tpg.setupOperationTDFD(set_op, request);
            if (txnOpId == 0) {
                headerOperation = set_op; //In TPG, this is the LD parent for all operations in the same txn
            }

            // addOperation an operation id for the operation for the purpose of temporal dependency construction
            set_op.setTxnOpId(txnOpId++);
            set_op.addHeader(headerOperation);
            headerOperation.addDescendant(set_op);
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
    }

    protected abstract void NOTIFY(Operation operation, Context context);

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

    private static int decodeInt(byte[] bytes, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value |= (bytes[offset + i] & 0xFF) << (i * 8);
        }
        return value;
    }

    // Method to encode string array into byte stream
    public static byte[] encodeStringArray(String[] stringArray) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(stringArray);
            return byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // Method to decode byte stream into string array
    public static String[] decodeStringArray(byte[] bytes) {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            Object object = objectInputStream.readObject();
            if (object instanceof String[]) {
                return (String[]) object;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

}
