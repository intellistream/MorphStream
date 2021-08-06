package transaction.scheduler.layered;

import content.T_StreamContent;
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

/**
 * breath-first-search based layered hash scheduler.
 */
@lombok.extern.slf4j.Slf4j
public class BFSLayeredHashScheduler extends Scheduler<OperationChain> {
    private int cacheIndex = 0;
    protected int delta;//range of each partition. depends on the number of op in the stage.

    public LayeredContext<HashMap<Integer, ArrayDeque<OperationChain>>> context;//<LevelID, ArrayDeque>

    public BFSLayeredHashScheduler(int tp, int NUM_ITEMS) {
        context = new LayeredContext<>(tp, HashMap::new);
        for (int threadId = 0; threadId < tp; threadId++) {
            context.layeredOCBucketGlobal.put(threadId, context.createContents());
        }
        delta = (int) Math.ceil(NUM_ITEMS / (double) tp); // Check id generation in DateGenerator.
    }

    /**
     * Build buckets with submitted ocs.
     * Return the local maximal dependency level.
     *
     * @param OCBucketThread
     * @param ocs
     * @return
     */
    public int buildBucketPerThread(HashMap<Integer, ArrayDeque<OperationChain>> OCBucketThread,
                                    Collection<OperationChain> ocs) {
        int localMaxDLevel = 0;
        for (OperationChain oc : ocs) {
            oc.updateDependencyLevel();
            int dependencyLevel = oc.getDependencyLevel();
            if (localMaxDLevel < dependencyLevel)
                localMaxDLevel = dependencyLevel;
            if (!OCBucketThread.containsKey(dependencyLevel))
                OCBucketThread.put(dependencyLevel, new ArrayDeque<>());
            OCBucketThread.get(dependencyLevel).add(oc);
        }
        log.debug("localMaxDLevel" + localMaxDLevel);
        return localMaxDLevel;
    }

    private void submit(int threadId, Collection<OperationChain> ocs) {
        context.totalOcsToSchedule[threadId] += ocs.size();

        for (OperationChain oc : ocs) {
            context.totalOsToSchedule[threadId] += oc.getOperations().size();
        }

        HashMap<Integer, ArrayDeque<OperationChain>> layeredOCBucketThread = context.layeredOCBucketGlobal.get(threadId);
        buildBucketPerThread(layeredOCBucketThread, ocs);
    }

    @Override
    public void INITIALIZE(int threadId) {
        Collection<Holder_in_range> tablesHolderInRange = context.getHolder().values();
        for (Holder_in_range tableHolderInRange : tablesHolderInRange) {//for each table.
            submit(threadId, tableHolderInRange.rangeMap.get(threadId).holder_v1.values());
        }
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(threadId);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
    }

    // DD: Transfer event processing
    public void CT_Transfer_Fun(Operation operation, long previous_mark_ID, boolean clean) {
        // read
        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues(operation.bid);
        SchemaRecord preValues1 = operation.condition_records[1].content_.readPreValues(operation.bid);
        if (preValues == null) {
            log.info("Failed to read condition records[0]" + operation.condition_records[0].record_.GetPrimaryKey());
            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[0].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[0].content_).versions.entrySet()) {
                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            log.info("TRY reading:" + operation.condition_records[0].content_.readPreValues(operation.bid));//not modified in last round);
        }
        if (preValues1 == null) {
            log.info("Failed to read condition records[1]" + operation.condition_records[1].record_.GetPrimaryKey());
            log.info("Its version size:" + ((T_StreamContent) operation.condition_records[1].content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) operation.condition_records[1].content_).versions.entrySet()) {
                log.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + operation.bid);
            }
            log.info("TRY reading:" + ((T_StreamContent) operation.condition_records[1].content_).versions.get(operation.bid));//not modified in last round);
        }
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();
        final long sourceAssetValue = preValues1.getValues().get(1).getLong();

        //TODO: make the condition checking more generic in future.

        // DD: Transaction Operation is conditioned on both source asset and account balance. So the operation can depend on both.
        if (sourceAccountBalance > operation.condition.arg1
                && sourceAccountBalance > operation.condition.arg2
                && sourceAssetValue > operation.condition.arg3) {
            //read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            //apply function.
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
        } else {
            log.info("Process failed");
        }
    }

    private void CT_Depo_Fun(Operation operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues(operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(operation.column_id).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues(operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    //TODO: the following are mostly hard-coded.
    private void execute(int threadId, Operation operation, long mark_ID, boolean clean) {
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
            CT_Transfer_Fun(operation, mark_ID, clean);
        } else if (operation.accessType == READ_WRITE_COND_READ) {
            CT_Transfer_Fun(operation, mark_ID, clean);
            operation.record_ref.setRecord(operation.d_record.content_.readPreValues(operation.bid));//read the resulting tuple.
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
            } else
                log.info("ERROR!");
        } else
            log.info("ERROR 2!");
    }

    /**
     * Used by OCScheduler.
     *
     * @param threadId
     * @param operation_chain
     * @param mark_ID
     */
    public void execute(int threadId, MyList<Operation> operation_chain, long mark_ID) {
        Operation operation = operation_chain.pollFirst();
        while (operation != null) {
            execute(threadId, operation, mark_ID, false);
            operation = operation_chain.pollFirst();
        }
    }

    @Override
    public void PROCESS(int threadId, long mark_ID) {
        MeasureTools.BEGIN_SCHEDULE_NEXT_TIME_MEASURE(threadId);
        OperationChain next = next(threadId);
        MeasureTools.END_SCHEDULE_NEXT_TIME_MEASURE(threadId);

        if (next != null) {
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            execute(threadId, next.getOperations(), mark_ID);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(threadId);
            log.debug("finished process: " + next.toString());
        }
    }

    /**
     * Return the last operation chain of threadId at dLevel.
     *
     * @param threadId
     * @return
     */
    protected OperationChain BFSearch(int threadId) {
        ArrayDeque<OperationChain> ocs = context.layeredOCBucketGlobal.get(threadId).get(context.currentLevel[threadId]);
        System.out.println(threadId + "|" + Arrays.toString(context.currentLevel));
        OperationChain oc = null;
        if (ocs != null && ocs.size() > 0)
            oc = ocs.removeLast();
        return oc;
    }

    private OperationChain next(int threadId) {
        return context.ready_oc.remove(threadId);// if a null is returned, it means, we are done with this level!
    }

    @Override
    public void EXPLORE(int threadId) {
        OperationChain next = BFSearch(threadId);
        if (next != null) {
            context.scheduledOcsCount[threadId] += 1;
        } else if (!context.finished(threadId)) {
            while (next == null) {
                context.currentLevel[threadId] += 1;
                next = BFSearch(threadId);
                SOURCE_CONTROL.getInstance().waitForOtherThreads();
            }
            context.scheduledOcsCount[threadId] += 1;
        }
        DISTRIBUTE(next, threadId);
    }
    @Override
    public void RESET() {
        SOURCE_CONTROL.getInstance().oneThreadCompleted();
    }

    //TODO: key divide by key range to determine responsible thread.
    private int getTaskId(String key) {
        Integer _key = Integer.valueOf(key);
        return _key / delta;
    }

    private OperationChain getOC(String tableName, String pKey) {

        ConcurrentHashMap<String, OperationChain> holder = context.getHolder(tableName).rangeMap.get(getTaskId(pKey)).holder_v1;
        return holder.computeIfAbsent(pKey, s -> new OperationChain(tableName, pKey));
    }

    private void checkDataDependencies(OperationChain dependent, Operation op, int thread_Id, String table_name,
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
        cacheIndex = cacheIndex % 4;
    }

    @Override
    public boolean SubmitRequest(Request request) {
        long bid = request.txn_context.getBID();
        OperationChain oc = getOC(request.table_name, request.d_record.record_.GetPrimaryKey());
        Operation operation = new Operation(request.table_name, request.s_record, request.d_record, request.record_ref, bid, request.accessType,
                request.function, request.condition_records, request.condition, request.txn_context, request.success);
        oc.addOperation(operation);
        checkDataDependencies(oc, operation, request.txn_context.thread_Id, request.table_name, request.src_key, request.condition_sourceTable, request.condition_source);
        return true;
    }
    @Override
    public void TxnSubmitBegin(int thread_Id) {
    }

    @Override
    public void TxnSubmitFinished(int thread_Id) {
    }

    @Override
    public boolean FINISHED(int threadId) {
        return context.finished(threadId);
    }

    @Override
    protected void DISTRIBUTE(OperationChain task, int threadId) {
        context.ready_oc.put(threadId, task);
    }
}