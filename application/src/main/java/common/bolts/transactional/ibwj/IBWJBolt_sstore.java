package common.bolts.transactional.ibwj;

import combo.SINKCombo;
import common.bolts.transactional.sl.GlobalSorter;
import common.param.TxnEvent;
import common.param.ibwj.IBWJEvent;
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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;

/**
 * Different from OLB, each executor in SStore has an associated partition id.
 */
public class IBWJBolt_sstore extends IBWJBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(IBWJBolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<Tuple> tuples = new ArrayDeque<>();

    public IBWJBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public IBWJBolt_sstore(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {
//        prepareEvents();
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock,
                this.context.getNUMTasks());
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

    public void start_evaluate(int thread_Id, double mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        transactionManager.stage.getControl().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        LA_RESETALL(transactionManager, thread_Id);
        // add bid_array for events
        if (thread_Id == 0) {
            int partitionOffset = config.getInt("NUM_ITEMS") / tthread;
            int[] p_bids = new int[(int) tthread];
            HashMap<Integer, Integer> pids = new HashMap<>();
            for (TxnEvent event : GlobalSorter.sortedEvents) {
                parseIBWJEvent(partitionOffset, (IBWJEvent) event, pids);
                event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                pids.replaceAll((k, v) -> p_bids[k]++);
                pids.clear();
            }
            GlobalSorter.sortedEvents.clear();
        }
        transactionManager.stage.getControl().postStateAccessBarrier(thread_Id);
    }

    private void parseIBWJEvent(int partitionOffset, IBWJEvent event, HashMap<Integer, Integer> pids) {
        pids.put((int) (Long.parseLong(event.getKey()) / partitionOffset), 0);
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
        //ensures that locks are added in the input_event sequence order.
//        LOG.info("+++++++thread_Id: " + thread_Id + " pid: " + _pid + " num partitions: " + event.num_p() + " partition index: " + Arrays.toString(event.partition_indexs) +
//                " bid array: " + Arrays.toString(event.getBid_array()) + " lock: " + transactionManager.getOrderLock(_pid).bid);
//        LA_LOCK(_pid, event.num_p(), transactionManager, event.getBid_array(), _bid, tthread);
        LA_LOCK_Reentrance(transactionManager, event.getBid_array(), event.partition_indexs, _bid, thread_Id);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        IBWJ_LOCK_AHEAD((IBWJEvent) event, txn_context[0]); //lock record's corresponding partition

        END_LOCK_TIME_MEASURE_ACC(thread_Id);
//      LA_UNLOCK(_pid, event.num_p(), transactionManager, tthread);
        LA_UNLOCK_Reentrance(transactionManager, event.partition_indexs, thread_Id);
        END_WAIT_TIME_MEASURE_ACC(thread_Id);
    }
}
