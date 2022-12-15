package common.bolts.transactional.tp;

import combo.SINKCombo;
import common.bolts.transactional.sl.GlobalSorter;
import common.param.TxnEvent;
import common.param.lr.LREvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerSStore;
import utils.SOURCE_CONTROL;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_log;
import static profiler.MeasureTools.*;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_SSTORE extends TPBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_SSTORE.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<Tuple> tuples = new ArrayDeque<>();

    public TPBolt_SSTORE(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public TPBolt_SSTORE(int fid) {
        super(LOG, fid, null);

    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock,
                this.context.getNUMTasks());
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(),
                this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), transactionManager.stage);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            int num_events = tuples.size();
            start_evaluate(thread_Id, in.getBID(), num_events);
            // execute txn_
            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
            for (Tuple tuple : tuples) {
                PRE_EXECUTE(tuple);
                //begin transaction processing.
                LAL_PROCESS(_bid);
                PostLAL_process(_bid);
                //end transaction processing.
                POST_PROCESS(_bid, timestamp, 1);//otherwise deadlock.
            }
            MeasureTools.END_TXN_TIME_MEASURE(thread_Id, num_events);
            tuples.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);//otherwise deadlock.
        } else {
            // sort
            execute_ts_normal(in);
            tuples.add(in);
        }
    }

    public void start_evaluate(int thread_Id, double mark_ID, int num_events) {
        transactionManager.stage.getControl().preStateAccessBarrier(thread_Id);
        LA_RESETALL(transactionManager, thread_Id);
        // add bid_array for events
        if (thread_Id == 0) {
            int partitionOffset = config.getInt("NUM_ITEMS") / tthread;
            int[] p_bids = new int[(int) tthread];
            HashMap<Integer, Integer> pids = new HashMap<>();
            for (TxnEvent event : GlobalSorter.sortedEvents) {
                parseTollProcessingEvent(partitionOffset, (LREvent) event, pids);
                event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                pids.replaceAll((k, v) -> p_bids[k]++);
                pids.clear();
            }
            GlobalSorter.sortedEvents.clear();
        }
        transactionManager.stage.getControl().postStateAccessBarrier(thread_Id);
    }

    private void parseTollProcessingEvent(int partitionOffset, LREvent event, HashMap<Integer, Integer> pids) {
        pids.put((event.getPOSReport().getSegment() / partitionOffset), 0);
    }

    @Override
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
//            System.out.println("thread: "+thread_Id+", event_id: "+_bid);
            TxnEvent event = (TxnEvent) input_event;
            GlobalSorter.addEvent(event);
        }
    }

    @Override
    protected void LAL_PROCESS(double _bid) throws DatabaseException {
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        TxnEvent event = (TxnEvent) input_event;
        int _pid = (event).getPid();
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        LA_LOCK_Reentrance(transactionManager, event.getBid_array(), event.partition_indexs, _bid, thread_Id);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        REQUEST_LOCK_AHEAD((LREvent) event, txn_context[0]);
        END_LOCK_TIME_MEASURE_ACC(thread_Id);
        LA_UNLOCK_Reentrance(transactionManager, event.partition_indexs, thread_Id);
        END_WAIT_TIME_MEASURE_ACC(thread_Id);
    }
}
