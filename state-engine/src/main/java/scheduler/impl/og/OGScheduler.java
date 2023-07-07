package scheduler.impl.og;


import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import scheduler.Request;
import scheduler.context.og.OGSchedulerContext;
import scheduler.impl.IScheduler;
import scheduler.struct.AbstractOperation;
import scheduler.struct.og.Operation;
import scheduler.struct.og.OperationChain;
import scheduler.struct.og.TaskPrecedenceGraph;
import scheduler.struct.op.MetaTypes;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.IntDataBox;
import transaction.function.*;
import transaction.impl.ordered.MyList;
import utils.AppConfig;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import static common.CONTROL.*;
import static content.common.CommonMetaTypes.AccessType.*;

public abstract class OGScheduler<Context extends OGSchedulerContext>
        implements IScheduler<Context> {
    private static final Logger log = LoggerFactory.getLogger(OGScheduler.class);
    public final int delta;//range of each partition. depends on the number of op in the stage.
    public final TaskPrecedenceGraph<Context> tpg; // TPG to be maintained in this global instance.

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

    /**
     * Transfer event processing
     *
     * @param operation
     * @param previous_mark_ID
     * @param clean
     */
    protected void Transfer_Fun(Operation operation, double previous_mark_ID, boolean clean) {
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
//            if (enable_log) log.debug("++++++ operation failed: "
//                    + sourceAccountBalance + "-" + operation.condition.arg1
//                    + " : " + sourceAccountBalance + "-" + operation.condition.arg2
////                    + " : " + sourceAssetValue + "-" + operation.condition.arg3
//                    + " condition: " + operation.condition);
//        }
    }

    /**
     * Deposite event processing
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    protected void Depo_Fun(Operation operation, long mark_ID, boolean clean) {
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues((long) operation.bid);
        List<DataBox> values = srcRecord.getValues();
        AppConfig.randomDelay();
        //apply function to modify..
        SchemaRecord tempo_record;
        tempo_record = new SchemaRecord(values);//tempo record
        tempo_record.getValues().get(1).incLong(operation.function.delta_long);//compute.
        operation.s_record.content_.updateMultiValues((long) operation.bid, mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
    }

    protected void GrepSum_Fun(Operation operation, double previous_mark_ID, boolean clean) {
        int keysLength = operation.condition_records.length;
        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.length];

        long sum = 0;

        // apply function
        AppConfig.randomDelay();

        for (int i = 0; i < keysLength; i++) {
            preValues[i] = operation.condition_records[i].content_.readPreValues((long) operation.bid);
            sum += preValues[i].getValues().get(1).getLong();
        }

        sum /= keysLength;

        if (operation.function.delta_long != -1) {
            // read
            SchemaRecord srcRecord = operation.s_record.content_.readPreValues((long) operation.bid);
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            // apply function

            if (operation.function instanceof SUM) {
//                tempo_record.getValues().get(1).incLong(tempo_record, sum);//compute.
                tempo_record.getValues().get(1).setLong(sum);//compute.
            } else
                throw new UnsupportedOperationException();
            operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            synchronized (operation.success) {
                operation.success[0]++;
            }
        }
    }

    // ED: Tweet Registrant - Asy_ModifyRecord
    protected void TweetRegistrant_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
//        AppConfig.randomDelay();
        // read
        SchemaRecord tweetRecord = operation.s_record.content_.readPastValues((long) operation.bid);
        if (tweetRecord == null) {
            log.info("TR: Empty tweet record not found");
            throw new NoSuchElementException();
        }
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
//        AppConfig.randomDelay();
        // read
        SchemaRecord wordRecord = operation.s_record.content_.readPastValues((long) operation.bid);
        if (wordRecord == null) {
            log.info("WU: Word record not found");
            throw new NoSuchElementException();
        }
        SchemaRecord tempo_record = new SchemaRecord(wordRecord); //tempo record
        final long oldCountOccurWindow = wordRecord.getValues().get(3).getLong();

        if (oldCountOccurWindow != 0) { // word has appeared before
            final int oldLastOccurWindow = wordRecord.getValues().get(5).getInt();
            final long oldFrequency = wordRecord.getValues().get(6).getLong();

            // Update word's tweetList
            if (operation.function instanceof Append) {
                tempo_record.getValues().get(2).addItem(operation.function.item); //append new tweetID into word's tweetList
            } else throw new UnsupportedOperationException();

            // Update word's window info
            if (oldLastOccurWindow < operation.condition.arg1) { //oldLastOccurWindow less than currentWindow
                tempo_record.getValues().get(3).incLong(oldCountOccurWindow, 1); //countOccurWindow += 1
                tempo_record.getValues().get(5).setInt((int) operation.condition.arg1); //update lastOccurWindow to currentWindow
            }
            tempo_record.getValues().get(6).incLong(oldFrequency, 1); //inner-window frequency += 1

        } else { // word has not appeared before
            String[] tweetList = {operation.function.item};
            tempo_record.getValues().get(1).setString(operation.condition.stringArg1); //wordValue
            tempo_record.getValues().get(2).setStringList(Arrays.asList(tweetList)); //tweetList
            tempo_record.getValues().get(3).setLong(1); //countOccurWindow
            tempo_record.getValues().get(5).setInt((int) operation.condition.arg1); //lastOccurWindow
            tempo_record.getValues().get(6).setLong(1); //frequency
        }

        operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        synchronized (operation.success) {
            operation.success[0]++;
        }
    }


    // ED: Trend Calculate - Asy_ModifyRecord_Read
    protected void TrendCalculate_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
//        AppConfig.randomDelay();
        // read
        SchemaRecord wordRecord = operation.s_record.content_.readPastValues((long) operation.bid);
        if (wordRecord == null) {
            log.info("TC: Word record not found");
            throw new NoSuchElementException();
        }
        final long countOccurWindow = wordRecord.getValues().get(3).getLong();
        final double oldTfIdf = wordRecord.getValues().get(4).getDouble();
        final long frequency = wordRecord.getValues().get(6).getLong();

        // Compute word's tf-idf
        if (operation.function instanceof TFIDF) {
            if (frequency > 0) { //frequency=0 means the word has already been computed in the window
                SchemaRecord tempo_record = new SchemaRecord(wordRecord);
                int windowSize = (int) operation.condition.arg1;  //(avg) number of words in window
                int windowCount = (int) operation.condition.arg2; //number of windows so far
                double tf = (double) frequency / windowSize;
                double idf = -1 * (Math.log((double) countOccurWindow / windowCount));
                double newTfIdf = Math.abs(tf * idf);
//                if (newTfIdf >= tfIdfThreshold) {
////                    log.info("High TFIDF detected");
//                }
                double difference = newTfIdf - oldTfIdf;

                tempo_record.getValues().get(4).setDouble(newTfIdf); //update tf-idf
                tempo_record.getValues().get(6).setLong(0); //reset inner-window frequency to zero
                if (isBurstByDifference) {
                    tempo_record.getValues().get(7).setBool(difference >= diffTfIdfThreshold); //set isBurst accordingly
                } else {
                    tempo_record.getValues().get(7).setBool(newTfIdf >= tfIdfThreshold);
                }

                operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
            }
            synchronized (operation.success) {
                operation.success[0]++;
            }
        } else throw new UnsupportedOperationException();
    }


    //    int newClusterCount=0;
//    ArrayDeque<Double> simiArray = new ArrayDeque<>();
    // ED-SC: Similarity Calculator - Asy_ModifyRecord_Iteration_Read
    protected void SimilarityCalculate_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
//        AppConfig.randomDelay();
        // input tweet is burst
        if (!operation.condition.boolArg1) {
            log.info("Non-burst tweet passed in");
            throw new IllegalArgumentException();
        }
        // read
        SchemaRecord tweetRecord = operation.s_record.content_.readPastValues((long) operation.bid);
        if (tweetRecord == null) {
            log.info("SC: Condition tweet record not found");
            throw new NoSuchElementException();
        }
        SchemaRecord tempo_record = new SchemaRecord(tweetRecord);
        HashMap<SchemaRecord, Double> similarities = new HashMap<>();

        String[] tweetWordList = tweetRecord.getValues().get(1).getStringList().toArray(new String[0]);
        HashMap<String, Integer> tweetMap = new HashMap<>();
        for (String word : tweetWordList) {tweetMap.put(word, 1);}

        if (operation.function instanceof Similarity) { // compute input tweet's cosine similarity with all clusters

            for (TableRecord record : operation.condition_records) { // iterate through all clusters in cluster_table
                // skip if the cluster has no update in the past two windows
                SchemaRecord clusterRecord = record.content_.readPastValues((long) operation.bid, (long) operation.bid - windowGap * tweetWindowSize);

                if (clusterRecord == null) { //skip record if it has not been updated in the past two windows
                    continue;
                }

                long clusterSize = clusterRecord.getValues().get(3).getLong();
                if (clusterSize != 0) { // cluster is not empty
                    try {
                        List<String> clusterWordList = new ArrayList<>(clusterRecord.getValues().get(1).getStringList());
                        String[] clusterWordArray = clusterWordList.toArray(new String[0]);

                        // compute cosine similarity
                        HashMap<String, Integer> clusterMap = new HashMap<>();
                        for (String word : clusterWordArray) {clusterMap.put(word, 1);}
                        Set<String> both = Sets.newHashSet(clusterMap.keySet());
                        both.retainAll(tweetMap.keySet());
                        double scalar = 0, norm1 = 0, norm2 = 0;
                        for (String k : both) scalar += clusterMap.getOrDefault(k, 0) * tweetMap.getOrDefault(k, 0);
                        for (String k : clusterMap.keySet()) norm1 += clusterMap.get(k) * clusterMap.get(k);
                        for (String k : tweetMap.keySet()) norm2 += tweetMap.get(k) * tweetMap.get(k);
                        double similarity = scalar / Math.sqrt(norm1 * norm2);

                        similarities.put(clusterRecord, similarity);
                    } catch (Exception e) {
                        log.info("Sync error when reading cluster records");
                    }

                }
            }

        } else {
            throw new UnsupportedOperationException();
        }

        boolean initNewCluster = true;

        if (similarities.size() > 0) {
            // determine the most similar cluster
            SchemaRecord maxCluster = Collections.max(similarities.entrySet(), Map.Entry.comparingByValue()).getKey();
            double maxSimilarity = similarities.get(maxCluster);
//            simiArray.add(maxSimilarity); //for testing

            if (maxSimilarity >= clusterSimiThreshold) { // Compare max similarity with threshold
//                log.info("Similar cluster found");
                tempo_record.getValues().get(2).setString(String.valueOf(maxCluster.getValues().get(0))); //update tweet.clusterID
                initNewCluster = false;
            }
        }

        if (initNewCluster) {
            int newClusterHashcode = Math.abs(Arrays.toString(tweetWordList).hashCode()) % 10007 % clusterTableSize; //TODO: Hashing clusterID to a fixed range
            String newClusterKey = String.valueOf(newClusterHashcode);
            tempo_record.getValues().get(2).setString(newClusterKey); //update tweet.clusterID
//            newClusterCount++;
//            log.info("New cluster counter: " + newClusterCount);
        }

        operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        synchronized (operation.success) {
            operation.success[0]++;
        }

    }

    // ED-CU: Cluster Updater - Asy_ModifyRecord
    protected void ClusterUpdate_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
//        AppConfig.randomDelay();
        SchemaRecord tweetRecord = operation.condition_records[0].content_.readPastValues((long) operation.bid);
        if (tweetRecord != null) {
            final List<String> tweetWordList = tweetRecord.getValues().get(1).getStringList();

            SchemaRecord clusterRecord = operation.s_record.content_.readPastValues((long) operation.bid);
            if (clusterRecord == null) {
                log.info("CU: Cluster record not found");
                throw new NoSuchElementException();
            }
            List<String> clusterWordList = clusterRecord.getValues().get(1).getStringList();
            SchemaRecord tempo_record = new SchemaRecord(clusterRecord); //tempo record
            // Merge input tweet into cluster
            for (String word : tweetWordList) {
                if (!clusterWordList.contains(word)) {clusterWordList.add(word);} //TODO: Replace set with map
            }

            tempo_record.getValues().get(1).setStringList(clusterWordList); //compute: merge wordList
            tempo_record.getValues().get(2).incLong(1); //compute: increment countNewTweet
            tempo_record.getValues().get(3).incLong(1); //compute: increment clusterSize

            //Update record's version (in this request, s_record == d_record)
            operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
//            log.info("Merged tweet " + tweetRecord.GetPrimaryKey() + " " + tweetWordList + " into cluster: " + clusterRecord.GetPrimaryKey() + " " + clusterWordList);
            synchronized (operation.success) {
                operation.success[0]++;
            }

        } else {
            log.info("CU: Condition tweet record not found");
            throw new NoSuchElementException();
        }

    }


    // ED-ES: Asy_ModifyRecord_Read
    protected void EventSelection_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
//        AppConfig.randomDelay();
        // read
        SchemaRecord clusterRecord = operation.s_record.content_.readPastValues((long) operation.bid);
        if (clusterRecord == null) {
            log.info("SC: Condition cluster record not found");
            throw new NoSuchElementException();
        }
        // cluster exits in clusterTable
        long countNewTweet = clusterRecord.getValues().get(2).getLong();
        long clusterSize = clusterRecord.getValues().get(3).getLong();
        try {
            SchemaRecord tempo_record = new SchemaRecord(clusterRecord); //tempo record

            // compute cluster growth rate
            if (operation.function instanceof Division) {
                double growthRate = (double) countNewTweet / clusterSize;
                if (isEventByGrowthRate) {
                    tempo_record.getValues().get(5).setDouble(growthRate); //update growthRate
                    tempo_record.getValues().get(4).setBool(growthRate > growthRateThreshold); //compute, update cluster.isEvent
                } else {
                    tempo_record.getValues().get(5).setDouble(countNewTweet); //update growthRate as numNewTweets
                    tempo_record.getValues().get(4).setBool(countNewTweet > countNewTweetThreshold);
                }
                tempo_record.getValues().get(2).setLong(0); //compute, reset cluster.countNewTweet to zero

                operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
                synchronized (operation.success) {
                    operation.success[0]++;
                }

            } else {
                throw new UnsupportedOperationException();
            }
        } catch (Exception e) {
            log.info("Concurrent write to cluster, probably due to small cluster table size");
        }
    }

    protected void IBWJ_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {

        AppConfig.randomDelay();

        int keysLength = operation.condition_records.length;
        SchemaRecord[] matchingTuples = new SchemaRecord[keysLength];
        for (int i = 0; i < keysLength; i++) {
            matchingTuples[i] = operation.condition_records[i].content_.readPreValues((long) operation.bid);
        }

        final String matchingAddress = matchingTuples[0].getValues().get(1).getString(); //find one of the matching addresses
        if (matchingAddress == null) {
            log.info("IBWJ: No matching tuple found");
            return;
        }

        // read
        SchemaRecord sourceTuple = operation.s_record.content_.readPastValues((long) operation.bid);
        if (sourceTuple == null) {
            log.info("IBWJ: Source tuple not found");
            throw new NoSuchElementException();
        }
        SchemaRecord tempo_record = new SchemaRecord(sourceTuple); //tempo record

        if (operation.function instanceof Insert) {
            tempo_record.getValues().get(1).setString(operation.function.item); //update tuple's own index address
            tempo_record.getValues().get(2).setString(matchingAddress); //update tuple's matching index address
            operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.

            synchronized (operation.success) {
                operation.success[0]++;
            }

        } else {
            throw new UnsupportedOperationException();
        }

    }

    // LB - Asy_ModifyRecord_Iteration_Read
    protected void LoadBalancer_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {

        int keysLength = operation.condition_records.length;
        SchemaRecord[] preValues = new SchemaRecord[operation.condition_records.length];

        // apply function
        AppConfig.randomDelay();

        // read server loads
        for (int i = 0; i < keysLength; i++) {
            preValues[i] = operation.condition_records[i].content_.readPreValues((long) operation.bid);
        }

        // write to the least loaded server
        SchemaRecord srcRecord = operation.s_record.content_.readPreValues((long) operation.bid);
        SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
        tempo_record.getValues().get(1).incLong(1);//compute.
        operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
        synchronized (operation.success) {
            operation.success[0]++;
        }

//        // apply function
//        AppConfig.randomDelay();
//
//        HashMap<SchemaRecord, Long> counters = new HashMap<>();
//
//        if (operation.condition.boolArg1) { //input packet from a new connection
//            for (TableRecord record : operation.condition_records) { // iterate through all servers
//                SchemaRecord serverRecord = record.content_.readPastValues((long) operation.bid);
//                counters.put(serverRecord, serverRecord.getValues().get(1).getLong());
//            }
//            SchemaRecord minServer = Collections.min(counters.entrySet(), Map.Entry.comparingByValue()).getKey();
//            SchemaRecord tempo_record = new SchemaRecord(minServer);
//            tempo_record.getValues().get(1).incLong(1);
//
//            //TODO: Non-deterministic key? Util this stage, d_record is unknown and set to null during txn construction.
//            operation.d_record.content_.updateMultiValues((long) operation.bid, (long) previous_mark_ID, clean, tempo_record);//it may reduce NUMA-traffic.
//        }
//        synchronized (operation.success) {
//            operation.success[0]++;
//        }

    }


    /**
     * general operation execution entry method for all schedulers.
     *
     * @param operation
     * @param mark_ID
     * @param clean
     */
    public void execute(Operation operation, double mark_ID, boolean clean) {
        if (operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
            return; // return if the operation is already aborted
        }
        int success;
        if (operation.accessType.equals(READ_WRITE_COND_READ)) {
            success = operation.success[0];
            if (this.tpg.getApp() == 1) { //SL
                Transfer_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_tc")) {
                TrendCalculate_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_sc")) {
                SimilarityCalculate_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_es")) {
                EventSelection_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 6) {
                IBWJ_Fun(operation, mark_ID, clean);
            }
            // check whether needs to return a read results of the operation
//            if (operation.record_ref != null) {
//                operation.record_ref.setRecord(operation.d_record.content_.readPreValues((long) operation.bid));//read the resulting tuple.
//            }
            if (operation.record_ref != null) {
                if (this.tpg.getApp() == 1) {
                    operation.record_ref.setRecord(operation.d_record.content_.readPreValues((long) operation.bid));//read the resulting tuple.
                } else if (this.tpg.getApp() == 4) {
                    SchemaRecord updatedRecord = operation.d_record.content_.readPastValues((long) operation.bid);
                    if (updatedRecord != null) {
                        operation.record_ref.setRecord(updatedRecord);
//                        if (Objects.equals(operation.operator_name, "ed_sc")) {
//                            log.info("Updating record ref for SC");
//                        }
                    } else {
                        log.info(operation.operator_name + ": D_Record not found"); //TODO: Remove after testing
                        throw new NullPointerException();
                    }
                }
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
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_cu")) {//ed_cu
                ClusterUpdate_Fun(operation, mark_ID, clean);
            }
            // operation success check, number of operation succeeded does not increase after execution
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(READ_WRITE)) {
            if (this.tpg.getApp() == 1) { //SL
                Depo_Fun(operation, (long) mark_ID, clean);
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
//            GrepSum_Fun(operation, mark_ID, clean);
            if (this.tpg.getApp() == 0) {
                GrepSum_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 7) {
                LoadBalancer_Fun(operation, mark_ID, clean);
            }
            if (operation.record_ref != null) {
                operation.record_ref.setRecord(operation.d_record.content_.readPreValues((long) operation.bid));//read the resulting tuple.
            }
            if (operation.success[0] == success) {
                operation.isFailed = true;
            }
        } else if (operation.accessType.equals(READ_WRITE_READ)) {
            assert operation.record_ref != null;
            AppConfig.randomDelay();
            List<DataBox> srcRecord = operation.s_record.record_.getValues();
            if (operation.function instanceof AVG) {
                success = operation.success[0];
                if (operation.condition.arg1 < operation.condition.arg2) {
                    double latestAvgSpeeds = srcRecord.get(1).getDouble();
                    double lav;
                    if (latestAvgSpeeds == 0) {//not initialized
                        lav = operation.function.delta_double;
                    } else
                        lav = (latestAvgSpeeds + operation.function.delta_double) / 2;

                    srcRecord.get(1).setDouble(lav);//write to state.
                    operation.record_ref.setRecord(new SchemaRecord(new DoubleDataBox(lav)));//return updated record.
                    synchronized (operation.success) {
                        operation.success[0]++;
                    }
                }
                if (operation.success[0] == success) {
                    operation.isFailed = true;
                }
            } else {
                HashSet cnt_segment = srcRecord.get(1).getHashSet();
                cnt_segment.add(operation.function.delta_int);//update hashset; updated state also. TODO: be careful of this.
                operation.record_ref.setRecord(new SchemaRecord(new IntDataBox(cnt_segment.size())));//return updated record.
            }
        } else if (operation.accessType.equals(WRITE_ONLY)) {
            //OB-Alert
            AppConfig.randomDelay();
            operation.d_record.record_.getValues().get(1).setLong(operation.value);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void PROCESS(Context context, double mark_ID) {
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

    public boolean executeWithBusyWait(Context context, OperationChain operationChain, double mark_ID) {
        MyList<Operation> operation_chain_list = operationChain.getOperations();
        for (Operation operation : operation_chain_list) {
            if (operation.getOperationState().equals(MetaTypes.OperationStateType.EXECUTED)
                    || operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)
                    || operation.isFailed) continue;
            if (isConflicted(context, operationChain, operation)) {
                return false;
            }
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            execute(operation, mark_ID, false);
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.thisThreadId);
            if (!operation.isFailed && !operation.getOperationState().equals(MetaTypes.OperationStateType.ABORTED)) {
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
        context.waitForOtherThreads(context.thisThreadId);
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
        double bid = request.txn_context.getBID();
        Operation set_op;
        Context targetContext = getTargetContext(request.src_key);
        switch (request.accessType) {
            case WRITE_ONLY:
                set_op = new Operation(request.src_key, null, request.table_name, null, null, null,
                        null, request.txn_context, request.accessType, request.operator_name, null, request.d_record, bid, targetContext);
                set_op.value = request.value;
                break;
            case READ_WRITE_COND: // they can use the same method for processing
            case READ_WRITE:
                set_op = new Operation(request.src_key, request.function, request.table_name, null, request.condition_records, request.condition,
                        request.success, request.txn_context, request.accessType, request.operator_name, request.d_record, request.d_record, bid, targetContext);
                break;
            case READ_WRITE_COND_READ:
            case READ_WRITE_COND_READN:
                set_op = new Operation(request.src_key, request.function, request.table_name, request.record_ref, request.condition_records, request.condition,
                        request.success, request.txn_context, request.accessType, request.operator_name, request.d_record, request.d_record, bid, targetContext);
                break;
            case READ_WRITE_READ:
                set_op = new Operation(request.src_key, request.function, request.table_name, request.record_ref, null, request.condition,
                        request.success, request.txn_context, request.accessType, request.operator_name, request.d_record, request.d_record, bid, targetContext);
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
                            || conditioned_operation.isFailed)) {
                        // blocked and busy wait
                        context.busyWaitQueue.add(operationChain);
                        return true;
                    }
                }
            }
        }
        return false;
    }

}
