package common.bolts.transactional.ed.wu;

import combo.SINKCombo;
import common.param.ed.wu.WUEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.impl.TxnManagerNoLock;

import java.util.ArrayDeque;
import java.util.Map;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.tweetWindowSize;

public class WUBolt_nocc extends WUBolt{
    private static final Logger LOG= LoggerFactory.getLogger(WUBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<Tuple> tuples;
    ArrayDeque<Tuple> outWindowEvents;
    private double windowBoundary;

    public WUBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }

    public WUBolt_nocc(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
        tuples = new ArrayDeque<>();
        outWindowEvents = new ArrayDeque<>();
        windowBoundary = tweetWindowSize;
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {}

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        double bid = in.getBID();
//        LOG.info("Thread " + this.thread_Id + " has event " + bid);

        if (bid >= windowBoundary) {
//            LOG.info("Thread " + this.thread_Id + " detects out-window event: " + in.getBID());
            outWindowEvents.add(in);
        } else {
            tuples.add(in);
        }

        if (outWindowEvents.size() == tthread) { //no more current-window-events in all receive_queues
            LOG.info("Thread " + this.thread_Id + " has reached punctuation: " + windowBoundary);
            int num_events = tuples.size();

            transactionManager.stage.getControl().preStateAccessBarrier(thread_Id);//await for all threads to reach window boundary

            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id); //TODO: Check measure methods. Modify so that it can sum up the exe time for multiple windows.
            for (Tuple tuple : tuples) {
                PRE_EXECUTE(tuple); //update input_tuple for TXN_PROCESS and POST_PROCESS
                TXN_PROCESS(_bid);
                POST_PROCESS(_bid, timestamp, combo_bid_size);
            }
            MeasureTools.END_TXN_TIME_MEASURE(thread_Id, num_events);

            tuples.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE(thread_Id);

            //normal-process the previous out-of-window events
            while (!outWindowEvents.isEmpty()) {
                Tuple outWindowTuple = outWindowEvents.poll();
                if (outWindowTuple.getBID() >= total_events) {//if the out-of-window events are stopping signals, directly pass to downstream
                    WORD_UPDATE_REQUEST_POST((WUEvent) outWindowTuple.getValue(0));
                    if (outWindowEvents.isEmpty()) { //stop itself when all stopping signals are posted
                        this.context.stop_running();
                    }

                } else { //otherwise, continue with normal-processing
                    tuples.add(in);
                }
            }

            windowBoundary += tweetWindowSize;
//            LOG.info("Thread " + this.thread_Id + " increment window boundary to: " + windowBoundary);

        }

    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            WU_TXN_PROCESS((WUEvent) input_event, i, _bid);
        }
    }

    private void WU_TXN_PROCESS(WUEvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
//        WORD_UPDATE_REQUEST(input_event, txn_context[(int) (i - _bid)]);
        WORD_UPDATE_REQUEST(input_event, txnContext);
        WORD_UPDATE_REQUEST_CORE(input_event);
    }

}
