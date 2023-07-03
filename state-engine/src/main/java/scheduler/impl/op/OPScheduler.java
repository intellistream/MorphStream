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

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

import static common.CONTROL.*;
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
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_sc")) {
                SimilarityCalculate_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 4 && Objects.equals(operation.operator_name, "ed_es")) {
                EventSelection_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 6) {
                IBWJ_Fun(operation, mark_ID, clean);
            }
//            else if (this.tpg.getApp() == 7) {
//                LoadBalancer_Fun(operation, mark_ID, clean); //using read_write_cond_readN instead
//            }
            // check whether needs to return a read results of the operation
            if (operation.record_ref != null) {
                if (this.tpg.getApp() == 1) { //SL
                    operation.record_ref.setRecord(operation.d_record.content_.readPreValues((long) operation.bid));//read the resulting tuple.
                } else if (this.tpg.getApp() == 4) { //ED
                    SchemaRecord updatedRecord = operation.d_record.content_.readPastValues((long) operation.bid);
                    if (updatedRecord != null) {
                        operation.record_ref.setRecord(updatedRecord);
                        if (operation.record_ref.isEmpty()) { //for testing
                            System.out.println("Record ref error");
                        }
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
            if (this.tpg.getApp() == 0) {
                GrepSum_Fun(operation, mark_ID, clean);
            } else if (this.tpg.getApp() == 7) {
                LoadBalancer_Fun(operation, mark_ID, clean);
            }
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

    int newClusterCount=0;
    ArrayDeque<Double> simiArray = new ArrayDeque<>();

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
                    List<String> clusterWordList = new ArrayList<>(clusterRecord.getValues().get(1).getStringList()); //TODO: Created new list to avoid sync error
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
            simiArray.add(maxSimilarity); //for testing

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
            newClusterCount++;
            log.info("New cluster counter: " + newClusterCount);
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
            log.info("Merged tweet " + tweetRecord.GetPrimaryKey() + " " + tweetWordList + " into cluster: " + clusterRecord.GetPrimaryKey() + " " + clusterWordList);

            synchronized (operation.success) {
                operation.success[0]++;
            }

        } else {
            log.info("CU: Condition tweet record not found");
            throw new NoSuchElementException();
        }

    }


    int esCounter = 0;
    ArrayDeque<Double> growthRates = new ArrayDeque<>();
    // ED-ES: Asy_ModifyRecord_Read
    protected void EventSelection_Fun(AbstractOperation operation, double previous_mark_ID, boolean clean) {
//        AppConfig.randomDelay();

        // read
        SchemaRecord clusterRecord = operation.s_record.content_.readPastValues((long) operation.bid);

        if (clusterRecord == null) {
            log.info("SC: Condition cluster record not found");
            throw new NoSuchElementException();
        } else {
            esCounter++;
//            log.info("SC valid record count: " + esCounter);
        }

        // cluster exits in clusterTable
        long countNewTweet = clusterRecord.getValues().get(2).getLong();
        long clusterSize = clusterRecord.getValues().get(3).getLong();
        SchemaRecord tempo_record = new SchemaRecord(clusterRecord); //tempo record

        // compute cluster growth rate
        if (operation.function instanceof Division) {
            double growthRate = (double) countNewTweet / clusterSize;
            growthRates.add(growthRate);
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
