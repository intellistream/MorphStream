package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.param.ed.tc.TCEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.impl.TxnManagerNoLock;
import utils.lib.ConcurrentHashMap;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;

import static common.CONTROL.tweetWindowSize;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE;

public class TCGBolt_nocc extends TCGBolt{
    private static final Logger LOG = LoggerFactory.getLogger(TCGBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<Tuple> tuples;
    ArrayDeque<Tuple> outWindowEvents;
    static ConcurrentHashMap<String, Boolean> tweetBurstMap; //Maps tweetID -> isBurst
    static ConcurrentHashMap<String, TCEvent> tweetEventMap; //Maps tweetID -> One related TCEvent
    private double windowBoundary;
    private int tweetIDFloor;


    public TCGBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }

    public TCGBolt_nocc(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
        tuples = new ArrayDeque<>();
        tweetBurstMap = new ConcurrentHashMap<>();
        tweetEventMap = new ConcurrentHashMap<>();
        windowBoundary = tweetWindowSize;
        tweetIDFloor = 0;
        outWindowEvents = new ArrayDeque<>();
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {}

    //This is only used for TCG_nocc
    private void execute_normal(Tuple in) {
        tuples.add(in);
        TCEvent event = (TCEvent) in.getValue(0);
        tweetEventMap.putIfAbsent(event.getTweetID(), event);

        if (event.tweetIDList != null) {
            LOG.info("TweetIDList: " + Arrays.toString(event.tweetIDList));
            for (String tweetID : event.tweetIDList) {
                try {
                    if (Integer.parseInt(tweetID) < tweetIDFloor || Integer.parseInt(tweetID) >= windowBoundary) { //Only consider tweets in the current window
                        continue;
                    }
                    if (!Boolean.TRUE.equals(tweetBurstMap.get(tweetID))) { //Do not update when the value is "not-null and true"
                        tweetBurstMap.put(tweetID, event.isBurst);
                    }
                    assert tweetBurstMap.size() <= tweetWindowSize;
                } catch (Exception e) {
                    LOG.info("Invalid tweetID detected in list");
                }
            }
        } else {
            LOG.info("No tweetIDList found for event " + event.getBid());
            throw new NoSuchElementException();
        }
    }

    private void TC_GATE_REQUEST_POST() throws InterruptedException {

        int tweet_partition_interval = (int) Math.ceil(tweetWindowSize / (double) tthread);
        int tweet_left_bound = thread_Id * tweet_partition_interval + tweetIDFloor;
        int tweet_right_bound;
        if (thread_Id == tthread - 1) {//last executor need to handle left-over
            tweet_right_bound = tweetWindowSize + tweetIDFloor;
        } else {
            tweet_right_bound = (thread_Id + 1) * tweet_partition_interval + tweetIDFloor;
        }

        for (int i=tweet_left_bound; i<tweet_right_bound; i++) {
            try {
                BEGIN_POST_TIME_MEASURE(thread_Id);

                String tweetID = String.valueOf(i);
                TCEvent event = tweetEventMap.get(tweetID);
                Boolean isBurst = tweetBurstMap.get(tweetID);

                if (isBurst == null || event == null) {throw new NoSuchElementException();}

                tweetEventMap.remove(tweetID);
                tweetBurstMap.remove(tweetID); //TODO: Improve this to make it faster

                TC_GATE_REQUEST_POST(event, isBurst);
                END_POST_TIME_MEASURE(thread_Id);
            } catch (Exception e) {
                LOG.info("No Such Element");
            }

        }

    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        double bid = in.getBID();
        LOG.info("Thread " + this.thread_Id + " has event " + bid);

        if (bid >= windowBoundary) {
//            LOG.info("Thread " + this.thread_Id + " detects out-window event: " + in.getBID());
            outWindowEvents.add(in);
        } else {
            execute_normal(in);
        }

        if (outWindowEvents.size() == tthread) { //no more current-window-events in all receive_queues
            LOG.info("Thread " + this.thread_Id + " has reached punctuation: " + windowBoundary);
            int num_events = tuples.size();

            transactionManager.stage.getControl().preStateAccessBarrier(thread_Id);//await for all threads to reach window boundary
            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id); //TODO: Check measure methods. Modify so that it can sum up the exe time for multiple windows.

            TC_GATE_REQUEST_POST(); //TCG does not perform txn processing.

            MeasureTools.END_TXN_TIME_MEASURE(thread_Id, num_events);

            tuples.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE(thread_Id);

            //normal-process the previous out-of-window events
            while (!outWindowEvents.isEmpty()) {
                Tuple outWindowTuple = outWindowEvents.poll();
                if (outWindowTuple.getBID() >= total_events) {//if the out-of-window events are stopping signals, directly pass to downstream
                    TC_GATE_REQUEST_POST((TCEvent) outWindowTuple.getValue(0), true); //TODO: Remove hardcode
                    if (outWindowEvents.isEmpty()) { //stop itself when all stopping signals are posted
                        this.context.stop_running();
                    }

                } else { //otherwise, continue with normal-processing
                    execute_normal(in);
                }
            }

            tweetIDFloor += tweetWindowSize;
            windowBoundary += tweetWindowSize;

        }

    }

}
