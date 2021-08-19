package transaction.scheduler.layered;

import index.high_scale_lib.ConcurrentHashMap;
import profiler.MeasureTools;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.IntDataBox;
import storage.datatype.ListDoubleDataBox;
import transaction.Holder_in_range;
import transaction.dedicated.ordered.MyList;
import transaction.function.*;
import transaction.scheduler.Request;
import transaction.scheduler.Scheduler;
import transaction.scheduler.layered.struct.Operation;
import transaction.scheduler.layered.struct.OperationChain;
import utils.SOURCE_CONTROL;

import java.util.*;

import static common.meta.CommonMetaTypes.AccessType.*;
import static java.lang.Integer.max;
import static java.lang.Integer.min;

/**
 * breath-first-search based layered hash scheduler.
 */
@lombok.extern.slf4j.Slf4j
public class BFSLayeredHashScheduler<Context extends LayeredContext> extends Scheduler<Context, OperationChain> {
    protected final int delta;//range of each partition. depends on the number of op in the stage.
    public int targetRollbackLevel;//shared data structure.
    private ConcurrentHashMap<String, Holder_in_range> holder_by_stage;//shared data structure.

    public BFSLayeredHashScheduler(int tp, int NUM_ITEMS) {
        delta = (int) Math.ceil(NUM_ITEMS / (double) tp); // Check id generation in DateGenerator.
        //create holder.
        holder_by_stage = new ConcurrentHashMap<>();
        holder_by_stage.put("accounts", new Holder_in_range(tp));
        holder_by_stage.put("bookEntries", new Holder_in_range(tp));
    }

    public Holder_in_range getHolder(String table_name) {
        return holder_by_stage.get(table_name);
    }

    public ConcurrentHashMap<String, Holder_in_range> getHolder() {
        return holder_by_stage;
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param OCBucketThread
     * @param ocs
     * @return
     */
    public int buildBucketPerThread(HashMap<Integer, ArrayList<OperationChain>> OCBucketThread,
                                    Collection<OperationChain> ocs) {
        int localMaxDLevel = 0;
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!OCBucketThread.containsKey(dependencyLevel))
                OCBucketThread.put(dependencyLevel, new ArrayList<>());
            OCBucketThread.get(dependencyLevel).add(oc);
        }
//        log.debug("localMaxDLevel" + localMaxDLevel);
        return localMaxDLevel;
    }

    private void submit(Context context, Collection<OperationChain> ocs) {
        for (OperationChain oc : ocs) {
            context.totalOsToSchedule += oc.getOperations().size();
        }
        HashMap<Integer, ArrayList<OperationChain>> layeredOCBucketThread = context.layeredOCBucketGlobal;
        context.maxLevel = buildBucketPerThread(layeredOCBucketThread, ocs);
    }

    @Override
    public void INITIALIZE(Context context) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        int threadId = context.thisThreadId;
        Collection<Holder_in_range> tablesHolderInRange = getHolder().values();
        for (Holder_in_range tableHolderInRange : tablesHolderInRange) {//for each table.
            submit(context, tableHolderInRange.rangeMap.get(threadId).holder_v1.values());
        }
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

//    // DD: Transfer event processing
//    public void CT_Transfer_Fun(Operation operation, long previous_mark_ID, boolean clean) {
//        // read
//        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
//        SchemaRecord preValues1 = operation.condition_records[1].content_.readPreValues(operation.bid);
//        if (preValues == null) {
//            log.info("Failed to read condition records[0]" + operation.condition_records[0].record_.GetPrimaryKey());
//            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[0].content_).versions.size());
//            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[0].content_).versions.entrySet()) {
//                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
//            }
//            log.info("TRY reading:" + operation.condition_records[0].content_.readPreValues(operation.bid));//not modified in last round);
//        }
//        if (preValues1 == null) {
//            log.info("Failed to read condition records[1]" + operation.condition_records[1].record_.GetPrimaryKey());
//            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[1].content_).versions.size());
//            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[1].content_).versions.entrySet()) {
//                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
//            }
//            log.info("TRY reading:" + ((T_StreamContent) operation.condition_records[1].content_).versions.get(operation.bid));//not modified in last round);
//        }
//        final long sourceAccountBalance = preValues.getValues().get(1).getLong();
//        final long sourceAssetValue = preValues1.getValues().get(1).getLong();
//
//        //TODO: make the condition checking more generic in future.
//
//        // DD: Transaction Operation is conditioned on both source asset and account balance. So the operation can depend on both.
//        if (sourceAccountBalance > operation.condition.arg1
//                && sourceAccountBalance > operation.condition.arg2
//                && sourceAssetValue > operation.condition.arg3) {
//            //read
//            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
//            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
//            //apply function.
//            if (operation.function instanceof INC) {
//                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta_long);//compute.
//            } else if (operation.function instanceof DEC) {
//                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta_long);//compute.
//            } else
//                throw new UnsupportedOperationException();
//            operation.d_record.content_.updateMultiValues(operation.bid, previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
//            synchronized (operation.success) {
//                operation.success[0]++;
//            }
//        } else {
//            log.info("Process failed");
//        }
//    }

    //TODO: the following are mostly hard-coded.
    private void execute(int thisThreadId, Operation operation, long mark_ID, boolean clean) {

        if (operation.aborted) return;
        if (operation.accessType == READS_ONLY) {
            operation.records_ref.setRecord(operation.d_record);
        } else if (operation.accessType == READ_ONLY) {//used in MB.
            SchemaRecord schemaRecord = operation.d_record.content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType);
            operation.record_ref.setRecord(new SchemaRecord(schemaRecord.getValues()));//Note that, locking scheme allows directly modifying on original table d_record.
        } else if (operation.accessType == WRITE_ONLY) {//push evaluation down.
            if (operation.value_list != null) { //directly replace value_list --only used for MB.
                operation.d_record.content_.WriteAccess(operation.bid, mark_ID, clean, new SchemaRecord(operation.value_list));//it may reduce NUMA-traffic.
            } else { //update by column_id.
                operation.d_record.record_.getValues().get(operation.column_id).setLong(operation.value);
            }
        } else if (operation.accessType == READ_WRITE) {//read, modify, write.
            CT_Depo_Fun(operation, mark_ID, clean);//used in SL
        } else if (operation.accessType == READ_WRITE_COND) {//read, modify (depends on condition), write( depends on condition).
            //TODO: pass function here in future instead of hard-code it. Seems not trivial in Java, consider callable interface?
            int success = operation.success[0];
            CT_Transfer_Fun(thisThreadId, operation, mark_ID, clean);
            if (operation.success[0] == success) {//TODO: For test only!
                operation.aborted = true;
            }
        } else if (operation.accessType == READ_WRITE_COND_READ) {
            int success = operation.success[0];
            CT_Transfer_Fun(thisThreadId, operation, mark_ID, clean);
            if (operation.success[0] == success) {
                operation.aborted = true;
            } else {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
            }
            assert operation.record_ref.cnt == 1;
        } else if (operation.accessType == READ_WRITE_READ) {//used in PK, TP.
            assert operation.record_ref != null;
            //read source.
            List<DataBox> srcRecord = operation.s_record.content_.ReadAccess(operation.bid, mark_ID, clean, operation.accessType).getValues();
            //apply function.
            if (operation.function instanceof Mean) {
                // compute.
                ListDoubleDataBox valueList = (ListDoubleDataBox) srcRecord.get(1);
                double sum = srcRecord.get(2).getDouble();
                double[] nextDouble = operation.function.new_value;
                for (int j = 0; j < 50; j++) {
                    sum -= valueList.addItem(nextDouble[j]);
                    sum += nextDouble[j];
                }
                srcRecord.get(2).setDouble(sum);
                if (valueList.size() < 1_000) {//just added
                    operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(nextDouble[50 - 1])));
                } else {
                    operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(sum / 1_000)));
                }
            } else if (operation.function instanceof AVG) {//used by TP
                double latestAvgSpeeds = srcRecord.get(1).getDouble();
                double lav;
                if (latestAvgSpeeds == 0) {//not initialized
                    lav = operation.function.delta_double;
                } else
                    lav = (latestAvgSpeeds + operation.function.delta_double) / 2;
                srcRecord.get(1).setDouble(lav);//write to state.
                operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
            } else if (operation.function instanceof CNT) {//used by TP
                HashSet cnt_segment = srcRecord.get(1).getHashSet();
                cnt_segment.add(operation.function.delta_int);//update hashset; updated state also. TODO: be careful of this.
                operation.record_ref.setRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
            }
        }

    }

    /**
     * Used by BFSScheduler.
     *
     * @param context
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(Context context, MyList<Operation> operation_chain, long mark_ID) {
        Operation operation = operation_chain.pollFirst();
        while (operation != null) {

            Operation finalOperation = operation;
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);

            execute(context.thisThreadId, finalOperation, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);

            if (operation.aborted) {
                context.abortedOperations.push(operation);
                context.aborted = true;
            }
            operation = operation_chain.pollFirst();

        }
    }

    @Override
    public void PROCESS(Context context, long mark_ID) {
        int threadId = context.thisThreadId;
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        OperationChain next = next(context);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
//            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            execute(context, next.getOperations(), mark_ID);
//            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
//            log.debug("finished execute current operation chain: " + next.toString());
        }
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param context
     * @return
     */
    protected OperationChain BFSearch(Context context) {
        ArrayList<OperationChain> ocs = context.BFSearch(); //
        OperationChain oc = null;
        if (ocs != null && context.currentLevelIndex < ocs.size()) {
            oc = ocs.get(context.currentLevelIndex++);
            context.scheduledOPs += oc.getOperations().size();
        }
        return oc;
    }

    private OperationChain next(Context context) {
        OperationChain operationChain = context.ready_oc;
        context.ready_oc = null;
        return operationChain;// if a null is returned, it means, we are done with this level!
    }

    @Override
    public void EXPLORE(Context context) {
        OperationChain next = BFSearch(context);
        if (next == null && !context.finished()) {//current level is all processed at the current thread.
            //Ready to proceed to next level
            //Check if there's any aborts
            if (context.aborted) {
                MarkOperationsToAbort(context);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                IdentifyRollbackLevel(context);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                SetRollbackLevel(context);
                RollbackToCorrectLayerForRedo(context);
                context.aborted = false;
            }
            while (next == null) {
                ProcessedToNextLevel(context);
                next = BFSearch(context);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
                //all threads come to the current level.
            }
        }
        DISTRIBUTE(next, context);
    }

    private void ProcessedToNextLevel(Context context) {
        context.currentLevel += 1;
        context.currentLevelIndex = 0;
    }

    private void SetRollbackLevel(Context context) {
        context.rollbackLevel = targetRollbackLevel;
    }

    private void IdentifyRollbackLevel(Context context) {
        if (context.thisThreadId == 0) {
            targetRollbackLevel = 0;
            for (int i = 0; i < context.totalThreads; i++) {
                targetRollbackLevel = max(targetRollbackLevel, context.rollbackLevel);
            }
        }
    }

    private void RollbackToCorrectLayerForRedo(Context context) {
        int level;
        for (level = context.rollbackLevel; level < context.currentLevel; level++) {
            context.scheduledOPs -= getNumOPsByLevel(context, level);
        }
        context.currentLevelIndex = 0;
        context.currentLevel = context.rollbackLevel;
        context.rollbackLevel = 0;
    }

    private int getNumOPsByLevel(Context context, int level) {
        int ops = 0;
        for (OperationChain operationChain : context.layeredOCBucketGlobal.get(level)) {
            ops += operationChain.getOperations().size();
        }
        return ops;
    }

    //TODO: mark operations of aborted transaction to be aborted.
    private void MarkOperationsToAbort(Context context) {
        boolean markAny = false;
        for (ArrayList<OperationChain> operationChains : context.layeredOCBucketGlobal.values()) {
            for (OperationChain operationChain : operationChains) {
                for (Operation operation : operationChain.getOperations()) {
                    markAny |= _MarkOperationsToAbort(context, operation);
                }
            }
            if (!markAny) {//current layer no one being marked.
                context.rollbackLevel++;
            }
        }
        context.rollbackLevel = min(context.rollbackLevel, context.currentLevel);
    }

    /**
     * Mark operations of an aborted transaction to abort.
     *
     * @param context
     * @param operation
     * @return
     */
    private boolean _MarkOperationsToAbort(Context context, Operation operation) {
        long bid = operation.bid;
        boolean markAny = false;
        //identify bids to be aborted.
        for (Operation op : context.abortedOperations) {
            if (bid == op.bid) {
                op.aborted = true;
                markAny = true;
            }
        }
        return markAny;
    }

    @Override
    public void RESET() {
        SOURCE_CONTROL.getInstance().oneThreadCompleted();
    }

    private OperationChain getOC(String tableName, String pKey) {
        ConcurrentHashMap<String, OperationChain> holder = getHolder(tableName).rangeMap.get(getTaskId(pKey, delta)).holder_v1;
        return holder.computeIfAbsent(pKey, s -> new OperationChain(tableName, pKey));
    }

    private void checkDataDependencies(OperationChain dependent, Operation op, String table_name,
                                       String key, String[] condition_sourceTable, String[] condition_source) {
        for (int index = 0; index < condition_source.length; index++) {
            if (table_name.equals(condition_sourceTable[index]) && key.equals(condition_source[index]))
                continue;// no need to check data dependency on a key itself.
            OperationChain dependency = getOC(condition_sourceTable[index], condition_source[index]);
            // dependency.getOperations().first().bid >= bid -- Check if checking only first ops bid is enough.
            if (dependency.getOperations().isEmpty() || dependency.getOperations().first().bid >= op.bid) {
                dependency.addPotentialDependent(dependent, op);
            } else {
                // All ops in transaction event involves writing to the states, therefore, we ignore edge case for read ops.
                dependent.addDependency(op, dependency); // record dependency
            }
        }
        dependent.checkOtherPotentialDependencies(op);
    }

    @Override
    public boolean SubmitRequest(Context context, Request request) {
        MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);
        long bid = request.txn_context.getBID();
        OperationChain oc = getOC(request.table_name, request.d_record.record_.GetPrimaryKey());
        Operation operation = new Operation(request.table_name, request.s_record, request.d_record, request.record_ref, bid, request.accessType,
                request.function, request.condition_records, request.condition, request.txn_context, request.success);
        oc.addOperation(operation);
        checkDataDependencies(oc, operation, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
        MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(context.thisThreadId);

        return true;
    }

    @Override
    public void TxnSubmitBegin(Context context) {
    }

    @Override
    public void TxnSubmitFinished(Context context) {
    }

    @Override
    public void AddContext(int thisTaskId, Context context) {
        throw new UnsupportedOperationException("not supported in bfs");
    }

    @Override
    public boolean FINISHED(Context context) {
        return context.finished();
    }

    @Override
    protected void DISTRIBUTE(OperationChain task, Context context) {
        context.ready_oc = task;
    }
}