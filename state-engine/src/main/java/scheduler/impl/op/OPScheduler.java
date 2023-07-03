package scheduler.impl.op;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scala.App;
import scheduler.Request;
import scheduler.impl.IScheduler;
import scheduler.context.op.OPSchedulerContext;
import scheduler.struct.AbstractOperation;
import scheduler.struct.op.MetaTypes;
import scheduler.struct.op.Operation;
import scheduler.struct.op.TaskPrecedenceGraph;
import scheduler.struct.op.WindowDescriptor;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.IntDataBox;
import transaction.function.*;
import utils.AppConfig;
import utils.SOURCE_CONTROL;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;

import static content.common.CommonMetaTypes.AccessType.*;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public abstract class OPScheduler<Context extends OPSchedulerContext, Task> implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OPScheduler.class);
    public final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph<Context> tpg; // TPG to be maintained in this global instance.
    public OPScheduler(int totalThreads, int NUM_ITEMS, int app) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) totalThreads); // Check id generation in DateGenerator.
        this.tpg = new TaskPrecedenceGraph<>(totalThreads, delta, NUM_ITEMS, app);
    }
    @Override
    public void initTPG(int offset) {
        tpg.initTPG(offset);
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

    public Context getTargetContext(String key) {
        // the thread to submit the operation may not be the thread to execute it.
        // we need to find the target context this thread is mapped to.
        int threadId =  Integer.parseInt(key) / delta;
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
//        log.trace("++++++execute: " + operation);
        // if the operation is in state aborted or committable or committed, we can bypass the execution
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED) || operation.isFailed) {
            //otherwise, skip (those +already been tagged as aborted).
            return;
        }
        int success;
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            success = operation.success[0];
            if (this.tpg.getApp() == 5) {
                SHJ_Fun(operation, mark_ID, clean);
            } else {
                Transfer_Fun(operation, mark_ID, clean);
            }
            // check whether needs to return a read results of the operation
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            success = operation.success[0];
            if (this.tpg.getApp() == 1) {//SL
                Transfer_Fun(operation, mark_ID, clean);
            } else {//OB
                AppConfig.randomDelay();
                List<DataBox> d_record = operation.condition_records[0].content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType).getValues();
                long askPrice = d_record.get(1).getLong();//price
                long left_qty = d_record.get(2).getLong();//available qty;
                long bidPrice = operation.condition.arg1;
                long bid_qty = operation.condition.arg2;
                if (bidPrice > askPrice || bid_qty < left_qty) {
                    d_record.get(2).setLong(left_qty - operation.function.delta_long);//new quantity.
                    operation.success[0]++;
                }
            }
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(READ_WRITE)) {
            if (this.tpg.getApp() == 1) {
                Depo_Fun(operation, mark_ID, clean);
            } else {
                AppConfig.randomDelay();
                SchemaRecord srcRecord = operation.s_record.content_.ReadAccess(operation.bid,mark_ID,clean,operation.accessType);
                List<DataBox> values = srcRecord.getValues();
                if (operation.function instanceof INC) {
                    values.get(2).setLong(values.get(2).getLong() + operation.function.delta_long);
                } else
                    throw new UnsupportedOperationException();
            }
        } else if (operation.accessType.equals(READ_WRITE_COND_READN)) {
            success = operation.success[0];
            GrepSum_Fun(operation, mark_ID, clean);
            // check whether needs to return a read results of the operation
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(WRITE_ONLY)) {
            //OB-Alert
            AppConfig.randomDelay();
            operation.d_record.record_.getValues().get(1).setLong(operation.value);
        } else if (operation.accessType.equals(READ_WRITE_READ)){
            assert operation.record_ref != null;
            AppConfig.randomDelay();
            List<DataBox> srcRecord = operation.s_record.record_.getValues();
            if (operation.function instanceof AVG) {
                double latestAvgSpeeds = srcRecord.get(1).getDouble();
                double lav;
                if (latestAvgSpeeds == 0) {//not initialized
                    lav = operation.function.delta_double;
                } else
                    lav = (latestAvgSpeeds + operation.function.delta_double) / 2;

                srcRecord.get(1).setDouble(lav);//write to state.
                operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
            } else {
                HashSet cnt_segment = srcRecord.get(1).getHashSet();
                cnt_segment.add(operation.function.delta_int);//update hashset; updated state also. TODO: be careful of this.
                operation.record_ref.setRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
            }
        } else if (operation.accessType.equals(WINDOWED_READ_ONLY)) {
            assert operation.record_ref != null;
            success = operation.success[0];
            AppConfig.randomDelay();
            Windowed_GrepSum_Fun(operation, mark_ID, clean);
            operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else {
            throw new UnsupportedOperationException();
        }

        assert operation.getOperationState() != MetaTypes.OperationStateType.EXECUTED;
    }

    protected void SHJ_Fun(AbstractOperation operation, long previous_mark_ID, boolean clean) {

//        AppConfig.randomDelay();

        // 1. Match: given a key, search windowed state R_key from R table, search windowed state S_key from S table.
        int keysLength = operation.condition_records.length;
        List<Long> rKeys = new ArrayList<>();
        for (int i = 0; i < keysLength; i++) {
            List<SchemaRecord> rSchemaRecordRange = operation.condition_records[i].content_.readPreValuesRange(operation.bid, 10240000);
            rKeys = rSchemaRecordRange.stream().map(schemaRecord -> schemaRecord.getValues().get(1).getLong()).collect(toList());
        }


        SchemaRecord sWindowedState = operation.s_record.content_.readPreValues((long) operation.bid);

        List<SchemaRecord> sSchemaRecordRange = operation.s_record.content_.readPreValuesRange(operation.bid, 10240000);
        List<Long> sKeys = sSchemaRecordRange.stream().map(schemaRecord -> schemaRecord.getValues().get(1).getLong()).collect(toList());

        // 2. Aggregation: given the matched results, apply aggregation function for stock exchange turnover rate.
        SchemaRecord tempo_record = new SchemaRecord(sWindowedState); //tempo record
        if (operation.function instanceof Join) {
            long rSum = rKeys.stream().mapToLong(rKey -> rKey).sum();
            long sSum = sKeys.stream().mapToLong(sKey -> sKey).sum();

            double turnoverRate = rSum < sSum ?
                        rSum / (double) sSum :
                        sSum / (double) rSum;

            log.info("++++++ Stock id: " + operation.pKey + " Turnover rate: " + turnoverRate);

            tempo_record.getValues().get(1).setLong(operation.function.delta_long);
            // 3. insert new tuple into S/R table.
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.

            synchronized (operation.success) {
                operation.success[0]++;
            }

        } else {
            throw new UnsupportedOperationException();
        }
        // 4: We do not need to delete old state from multi-versioned storage
    }

    // DD: Transfer event processing
    protected void Transfer_Fun(AbstractOperation operation, long previous_mark_ID, boolean clean) {
        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();

        // apply function
        AppConfig.randomDelay();

        if (sourceAccountBalance > operation.condition.arg1
                && sourceAccountBalance > operation.condition.arg2) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record

            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        }
//        else {
//            log.info("++++++ operation failed: "
//                    + sourceAccountBalance + "-" + operation.condition.arg1
//                    + " : " + sourceAccountBalance + "-" + operation.condition.arg2
////                    + " : " + sourceAssetValue + "-" + operation.condition.arg3
//                    + " condition: " + operation.condition);
//        }
    }

    protected void Depo_Fun(AbstractOperation operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        AppConfig.randomDelay();
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    protected void GrepSum_Fun(AbstractOperation operation, long previous_mark_ID, boolean clean) {
        int keysLength = operation.condition_records.length;
        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.length];

        long sum = 0;

        // apply function
        AppConfig.randomDelay();

        for (int i = 0; i < keysLength; i++) {
//            long start = System.nanoTime();
//            while (System.nanoTime() - start < 10000) {}
            preValues[i] = operation.condition_records[i].content_.readPreValues(operation.bid);
            sum += preValues[i].getValues().get(1).getLong();
        }

        sum /= keysLength;

        if (operation.function.delta_long != -1) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (operation.function instanceof SUM) {
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        }
//        else {
//            log.info("++++++ operation failed: " + operation);
//        }
    }

    protected void Windowed_GrepSum_Fun(AbstractOperation operation, long previous_mark_ID, boolean clean) {
        int keysLength = operation.condition_records.length;

        long sum = 0;

        // apply function
        AppConfig.randomDelay();


        for (int i = 0; i < keysLength; i++) {
//            long start = System.nanoTime();
//            while (System.nanoTime() - start < 10000) {}
            assert operation.windowContext.isWindowed();
            List<SchemaRecord> schemaRecordRange = operation.condition_records[i].content_.readPreValuesRange(operation.bid, operation.windowContext.getRange());
            sum += schemaRecordRange.stream().mapToLong(schemaRecord -> schemaRecord.getValues().get(1).getLong()).sum();
        }

        sum /= (long) keysLength * AppConfig.windowSize;

        if (operation.function.delta_long != -1) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (operation.function instanceof SUM) {
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        }
//        else {
//            log.info("++++++ operation failed: " + operation);
//        }
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
            long bid = request.txn_context.getBID();
            Operation set_op;
            switch (request.accessType) {
                case WRITE_ONLY:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, null, null, null, null);
                    set_op.value = request.value;
                    break;
                case READ_WRITE: // they can use the same method for processing
                case READ_WRITE_COND:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                case READ_WRITE_COND_READN:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_READ:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, null, null, null);
                    break;
                case WINDOWED_READ_ONLY:
                    WindowDescriptor windowContext = new WindowDescriptor(true, AppConfig.windowSize);
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success, windowContext);
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
//            set_op.setConditionSources(request.condition_sourceTable, request.condition_source);
            tpg.setupOperationTDFD(set_op, request);
            if (txnOpId == 0)
                headerOperation = set_op;
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

}
