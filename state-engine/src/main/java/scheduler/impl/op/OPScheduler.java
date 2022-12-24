package scheduler.impl.op;


import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.impl.IScheduler;
import scheduler.context.op.OPSchedulerContext;
import scheduler.struct.AbstractOperation;
import scheduler.struct.op.MetaTypes;
import scheduler.struct.op.Operation;
import scheduler.struct.op.TaskPrecedenceGraph;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.*;
import transaction.function.*;
import utils.AppConfig;
import utils.SOURCE_CONTROL;

import java.util.*;

import static content.common.CommonMetaTypes.AccessType.*;

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
    public void execute(Operation operation, double mark_ID, boolean clean) {
//        log.trace("++++++execute: " + operation);
        // if the operation is in state aborted or committable or committed, we can bypass the execution
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED) || operation.isFailed) {
            //otherwise, skip (those +already been tagged as aborted).
            return;
        }
        int success;
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            success = operation.success[0];
            if (this.tpg.getApp() == 1) { //SL
                Transfer_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_tc")) {
                TrendCalculate_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_es")) {
                EventSelection_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_cu")) {
                ClusterUpdate_Fun(operation, mark_ID, clean);
            }
            // check whether needs to return a read results of the operation
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues((long) operation.bid));//read the resulting tuple.
            }
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(READ_WRITE_COND)) {
            success = operation.success[0];
            if (this.tpg.getApp() == 1) {//SL
                Transfer_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 3) {//OB
                AppConfig.randomDelay();
                List<DataBox> d_record = operation.condition_records[0].content_.ReadAccess((long) operation.bid, (long) mark_ID, clean, operation.accessType).getValues();
                long askPrice = d_record.get(1).getLong();//price
                long left_qty = d_record.get(2).getLong();//available qty;
                long bidPrice = operation.condition.arg1;
                long bid_qty = operation.condition.arg2;
                if (bidPrice > askPrice || bid_qty < left_qty) {
                    d_record.get(2).setLong(left_qty - operation.function.delta_long);//new quantity.
                    operation.success[0]++;
                }
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_tr")) {//ed_tr
                TweetRegistrant_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_wu")) {//ed_wu
                WordUpdate_Fun(operation, mark_ID, clean);
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
                SchemaRecord srcRecord = operation.s_record.content_.ReadAccess((long) operation.bid, (long) mark_ID, clean, operation.accessType);
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
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues((long) operation.bid));//read the resulting tuple.
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
        } else {
            throw new UnsupportedOperationException();
        }

        assert operation.getOperationState() != MetaTypes.OperationStateType.EXECUTED;
    }

    // DD: Transfer event processing
    protected void Transfer_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
        SchemaRecord preValues = operation.condition_records[0].content_.readPreValues((long) operation.bid);
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();

        // apply function
        AppConfig.randomDelay();

        if (sourceAccountBalance > operation.condition.arg1
                && sourceAccountBalance > operation.condition.arg2) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues((long) operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record

            if (operation.function instanceof INC) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else if (operation.function instanceof DEC) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, operation.function.delta_long);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
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

    protected void Depo_Fun(AbstractOperation operation, double mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues((long) operation.bid);
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        AppConfig.randomDelay();
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues((long) operation.bid, (long) mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    protected void GrepSum_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
        int keysLength = operation.condition_records.length;
        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.length];

        long sum = 0;

        // apply function
        AppConfig.randomDelay();

        for (int i = 0; i < keysLength; i++) {
//            long start = System.nanoTime();
//            while (System.nanoTime() - start < 10000) {}
            preValues[i] = operation.condition_records[i].content_.readPreValues((long) operation.bid);
            sum += preValues[i].getValues().get(1).getLong();
        }

        sum /= keysLength;

        if (operation.function.delta_long != -1) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues((long) operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (operation.function instanceof SUM) {
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        }
//        else {
//            log.info("++++++ operation failed: " + operation);
//        }
    }

    // ED: Tweet Registrant - Asy_ModifyRecord
    protected void TweetRegistrant_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {

        // apply function
        AppConfig.randomDelay();

        // read
        SchemaRecord tweetRecord = operation.s_record.content_.readPastValues((long) operation.bid);
        SchemaRecord tempo_record = new SchemaRecord(tweetRecord); //tempo record

        // Update tweet's wordList
        if (operation.function instanceof Insert) {
            tempo_record.getValues().get(1).setStringList(Arrays.asList(operation.function.stringArray)); //compute, update wordList
        } else
            throw new UnsupportedOperationException();

        //Update record's version (in this request, s_record == d_record)
        operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        synchronized (operation.success) {
            operation.success[0]++;
        }

    }


    // ED: Word Update - Asy_ModifyRecord
    protected void WordUpdate_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
        SchemaRecord preValues = operation.condition_records[0].content_.readPastValues((long) operation.bid); //condition_record[0] stores the current word's record

        if (preValues != null) {
            final long oldCountOccurWindow = preValues.getValues().get(3).getLong();

            // apply function
            AppConfig.randomDelay();

            // read
            SchemaRecord wordRecord = operation.s_record.content_.readPastValues((long) operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(wordRecord); //tempo record

            if (oldCountOccurWindow != -1) { // word has been stored into table
                final int oldLastOccurWindow = preValues.getValues().get(5).getInt();
                final long oldFrequency = preValues.getValues().get(6).getLong();

                // Update word's tweetList
                if (operation.function instanceof Append) {
                    tempo_record.getValues().get(2).addItem(operation.function.item); //compute, append new tweetID into word's tweetList
                } else
                    throw new UnsupportedOperationException();

                // Update word's window info
                if (oldLastOccurWindow < operation.condition.arg1) { //oldLastOccurWindow less than currentWindow
                    tempo_record.getValues().get(3).incLong(oldCountOccurWindow, 1); //compute, increase countOccurWindow by 1
                    tempo_record.getValues().get(5).setInt((int) operation.condition.arg1); //compute, set lastOccurWindow to currentWindow
                }

                // Update word's inner-window frequency
                tempo_record.getValues().get(6).incLong((long) oldFrequency, (long) 1); //compute, increase word's frequency by 1

            } else { // word has not been stored into table

                String[] tweetList = {operation.function.item};

                tempo_record.getValues().get(1).setString(operation.condition.stringArg1); //wordValue
                tempo_record.getValues().get(2).setStringList(Arrays.asList(tweetList)); //tweetList
                tempo_record.getValues().get(3).setLong(1); //countOccurWindow
                tempo_record.getValues().get(4).setDouble(-1); //TF-IDF
                tempo_record.getValues().get(5).setInt((int) operation.condition.arg1); //lastOccurWindow
                tempo_record.getValues().get(6).setLong(1); //frequency
                tempo_record.getValues().get(7).setBool(false); //isBurst

            }

            //Update record's version (in this request, s_record == d_record)
            operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }

        }

    }



    // ED: Trend Calculate - Asy_ModifyRecord_Read
    protected void TrendCalculate_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {

        //Only READ word whose record is updated in the current window
        SchemaRecord preValues = operation.condition_records[0].content_.readCurrValues((long) operation.bid);

        if (preValues != null) {
            final int countOccurWindow = preValues.getValues().get(3).getInt();
            final double oldTfIdf = preValues.getValues().get(4).getDouble();
            final int frequency = preValues.getValues().get(6).getInt();

            // apply function
            AppConfig.randomDelay();

            // read
            SchemaRecord wordRecord = operation.s_record.content_.readCurrValues((long) operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(wordRecord); //tempo record

            // Compute word's tf-idf
            if (operation.function instanceof TFIDF) {
                int windowSize = (int) operation.condition.arg1; //window count
                int windowCount = (int) operation.condition.arg2; //window size
                double tf = (double) frequency / windowSize;
                double idf = -1 * (Math.log((double) countOccurWindow / windowCount));
                double newTfIdf = tf * idf;
                double difference = newTfIdf - oldTfIdf;

                tempo_record.getValues().get(4).setDouble(newTfIdf); //compute: update tf-idf
                tempo_record.getValues().get(6).setInt(0); //compute: reset frequency to zero

                if (difference >= 0.5) { //TODO: Check this threshold
                    tempo_record.getValues().get(7).setBool(true); //compute: set isBurst to true
                } else {
                    tempo_record.getValues().get(7).setBool(false); //compute: set isBurst to false
                }

                //Update record's version (in this request, s_record == d_record)
                operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
                synchronized (operation.success) {
                    operation.success[0]++;
                }

            } else
                throw new UnsupportedOperationException();
        }
    }

    // ED-CU: Cluster Update - Asy_ModifyRecord_Iteration
    protected void ClusterUpdate_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
        HashMap<SchemaRecord, Double> similarities = new HashMap<>();

        // apply function
        AppConfig.randomDelay();

        // read
        SchemaRecord tweetRecord = operation.s_record.content_.readPastValues((long) operation.bid);

        // input tweet is burst
        if (operation.condition.boolArg1) {
            String[] tweetWordList = tweetRecord.getValues().get(1).getStringList().toArray(new String[0]);
            HashMap<String, Integer> tweetMap = new HashMap<>();
            for (String word : tweetWordList) {tweetMap.put(word, 1);}

            // compute input tweet's cosine similarity with all clusters
            if (operation.function instanceof Similarity) {

                // iterate through all clusters in cluster_table
                for (TableRecord record : operation.condition_records) {

                    // skip if the cluster has no update in the past two windows
                    SchemaRecord clusterRecord = record.content_.readPastValues((long) operation.bid, (long) operation.bid-2);
                    if (clusterRecord == null) {continue;}

                    int clusterSize = clusterRecord.getValues().get(3).getInt();

                    if (clusterSize != -1) { // cluster is valid
                        String[] clusterWordList = clusterRecord.getValues().get(1).getStringList().toArray(new String[0]);

                        // compute cosine similarity
                        HashMap<String, Integer> clusterMap = new HashMap<>();
                        for (String word : clusterWordList) {clusterMap.put(word, 1);}
                        Set<String> both = Sets.newHashSet(clusterMap.keySet());
                        both.retainAll(tweetMap.keySet());
                        double scalar = 0, norm1 = 0, norm2 = 0;
                        for (String k : both) scalar += clusterMap.getOrDefault(k, 0) * tweetMap.getOrDefault(k, 0);
                        for (String k : clusterMap.keySet()) norm1 += clusterMap.get(k) * clusterMap.get(k);
                        for (String k : tweetMap.keySet()) norm2 += tweetMap.get(k) * tweetMap.get(k);
                        double similarity = scalar / Math.sqrt(norm1 * norm2);

                        similarities.put(clusterRecord, similarity);
                    }
                }

            } else {
                throw new UnsupportedOperationException();
            }

            // determine the most similar cluster
            SchemaRecord maxCluster = Collections.max(similarities.entrySet(), Map.Entry.comparingByValue()).getKey();
            double maxSimilarity = similarities.get(maxCluster);

            // Compare max similarity with threshold: 0.5
            if (maxSimilarity >= 0.5) { //TODO: Check the threshold value
                SchemaRecord tempo_record = new SchemaRecord(maxCluster); //tempo record - the most similar cluster
                List<String> wordList = maxCluster.getValues().get(1).getStringList();
                int countNewTweet = maxCluster.getValues().get(2).getInt();
                int clusterSize = maxCluster.getValues().get(3).getInt();

                // Merge input tweet into cluster
                for (String word : tweetWordList) {
                    if (!wordList.contains(word)) {wordList.add(word);}
                }

                tempo_record.getValues().get(1).setStringList(wordList); //compute: merge wordList
                tempo_record.getValues().get(2).setInt(countNewTweet + 1); //compute: increment countNewTweet
                tempo_record.getValues().get(3).setInt(clusterSize + 1); //compute: increment clusterSize

                //Update record's version (in this request, s_record == d_record)
                operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.

            } else { // Initialize a new cluster

                // Convert new cluster's wordList to clusterID
                int newClusterKey = Arrays.toString(tweetWordList).hashCode() % 1007;
                TableRecord newRecord = operation.condition_records[newClusterKey];
                SchemaRecord clusterRecord = newRecord.content_.readPastValues((long) operation.bid);
                SchemaRecord tempo_record = new SchemaRecord(clusterRecord);

                tempo_record.getValues().get(1).setStringList(Arrays.asList(tweetWordList)); // create wordList
                tempo_record.getValues().get(2).setInt(1); // countNewTweet = 1
                tempo_record.getValues().get(3).setInt(1); // clusterSize = 1
                tempo_record.getValues().get(4).setBool(false); // isEvent = false

                //Update record's version (in this request, s_record == d_record)
                operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.

            }
            synchronized (operation.success) {
                operation.success[0]++;
            }

        }

    }



    // ED-ES: Asy_ModifyRecord_Read
    protected void EventSelection_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {

        //Only READ cluster who has been updated in the current window
        SchemaRecord preValues = operation.condition_records[0].content_.readCurrValues((long) operation.bid);

        if (preValues != null) { // cluster exits in clusterTable
            int countNewTweet = preValues.getValues().get(3).getInt();
            int clusterSize = preValues.getValues().get(4).getInt();

            // apply function
            AppConfig.randomDelay();

            // read
            SchemaRecord clusterRecord = operation.s_record.content_.readCurrValues((long) operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(clusterRecord); //tempo record

            tempo_record.getValues().get(3).setInt(0); //compute, reset cluster.countNewTweet to zero

            // compute cluster growth rate
            if (operation.function instanceof Division) {
                double growthRate = (double) countNewTweet / clusterSize;

                //TODO: Check growth rate threshold
                tempo_record.getValues().get(5).setBool(growthRate > 0.5); //compute, update cluster.isEvent

                //Update record's version (in this request, s_record == d_record)
                operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
                synchronized (operation.success) {
                    operation.success[0]++;
                }

            } else
                throw new UnsupportedOperationException();

        }

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
        context.waitForOtherThreads(context.thisThreadId);
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
            double bid = request.txn_context.getBID();
            Operation set_op;
            switch (request.accessType) {
                case WRITE_ONLY:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType, request.operator_name,
                            request.d_record, null, null, null, null);
                    set_op.value = request.value;
                    break;
                case READ_WRITE: // they can use the same method for processing
                case READ_WRITE_COND:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType, request.operator_name,
                            request.d_record, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_COND_READ:
                case READ_WRITE_COND_READN:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType, request.operator_name,
                            request.d_record, request.record_ref, request.function, request.condition, request.condition_records, request.success);
                    break;
                case READ_WRITE_READ:
                    set_op = new Operation(request.src_key, getTargetContext(request.src_key), request.table_name, request.txn_context, bid, request.accessType, request.operator_name,
                            request.d_record, request.record_ref, request.function, null, null, null);
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

    public void start_evaluation(Context context, double mark_ID, int num_events) {
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
