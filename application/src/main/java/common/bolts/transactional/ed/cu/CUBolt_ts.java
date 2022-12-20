package common.bolts.transactional.ed.cu;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Similarity;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class CUBolt_ts extends CUBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CUBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<CUEvent> cuEvents;

    //To be used in Combo
    public CUBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public CUBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        cuEvents = new ArrayDeque<>();
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            CUEvent event = (CUEvent) input_event;
            if (enable_latency_measurement) {
                (event).setTimestamp(timestamp);
            }
            CLUSTER_UPDATE_REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    protected void CLUSTER_UPDATE_REQUEST_CONSTRUCT(CUEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        String[] clusterTable = new String[]{"cluster_table"}; //condition source table to iterate
        String[] clusterKey = new String[]{""}; //condition source key, set to null
        Similarity function = new Similarity();
        Condition condition1 = new Condition(event.getCurrWindow(), event.isBurst()); //arg1: currentWindow, boolArg1: isBurst

        LOG.info("Constructing CU request: " + event.getMyBid());

        transactionManager.BeginTransaction(txnContext);

        // Update cluster: merge input tweet into existing cluster, or initialize new cluster
        transactionManager.Asy_ModifyRecord_Iteration(
                txnContext,
                "tweet_table", // source_table
                event.getTweetID(),  // source_key
                function, // determine the most similar cluster
                clusterTable, clusterKey, //condition_source_table, condition_source_key
                condition1,
                event.success,
                "ed_cu_cluster"
        );

        transactionManager.CommitTransaction(txnContext);

        cuEvents.add(event);
    }

    private void CLUSTER_UPDATE_REQUEST_CORE() throws InterruptedException {
    }

    private void CLUSTER_UPDATE_REQUEST_POST() throws InterruptedException {
        for (CUEvent event : cuEvents) {
            CLUSTER_UPDATE_REQUEST_POST(event);
        }
    }

    private boolean doPunctuation() {
        return cuEvents.size() == tweetWindowSize / tthread;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (doPunctuation()) {
            int num_events = cuEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    CLUSTER_UPDATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    CLUSTER_UPDATE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                cuEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }
}
