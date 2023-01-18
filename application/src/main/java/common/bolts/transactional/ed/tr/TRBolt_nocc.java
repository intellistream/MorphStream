package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
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

public class TRBolt_nocc extends TRBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TRBolt_nocc.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<Tuple> tuples;
    private double windowBoundary;

    public TRBolt_nocc(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TRBolt_nocc(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerNoLock(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
        tuples = new ArrayDeque<>();
        windowBoundary = tweetWindowSize;
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {
        loadDB(null, context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException {

        double bid = in.getBID();
        LOG.info("Thread " + this.thread_Id + " has event " + bid);

        if (bid >= windowBoundary) { //Input event is the last event in the current window
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

            windowBoundary += tweetWindowSize;
//            LOG.info("Thread " + this.thread_Id + " increment window boundary to: " + windowBoundary);

            // Upon receiving stopping signal, pass it to downstream
            if (bid >= total_events) {
                EMIT_STOP_SIGNAL((TREvent) in.getValue(0));
                // Stop itself
                this.context.stop_running();
            }
        }

        MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); //TODO: Check where to put this
        tuples.add(in);
    }

    @Override
    protected void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TR_TXN_PROCESS((TREvent) input_event, i, _bid);
        }
    }

    private void TR_TXN_PROCESS(TREvent input_event, double i, double _bid) throws DatabaseException, InterruptedException {
        TxnContext txnContext = new TxnContext(thread_Id, this.fid, i); //TODO: Improve this, avoid initializing new txnContext everytime by making it as shared var
//        TWEET_REGISTRANT_REQUEST(input_event, txn_context[(int) (i - _bid)]);
        TWEET_REGISTRANT_REQUEST(input_event, txnContext);
        TWEET_REGISTRANT_REQUEST_CORE(input_event);
    }

}
