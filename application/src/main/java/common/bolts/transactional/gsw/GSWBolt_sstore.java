package common.bolts.transactional.gsw;

import combo.SINKCombo;
import common.bolts.transactional.sl.GlobalSorter;
import common.param.TxnEvent;
import common.param.gsw.WindowedMicroEvent;
import common.param.mb.MicroEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerSStore;
import utils.AppConfig;
import utils.SOURCE_CONTROL;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;

/**
 * Different from OLB, each executor in SStore has an associated partition id.
 */
public class GSWBolt_sstore extends GSWBolt_LA {
    private static final Logger LOG = LoggerFactory.getLogger(GSWBolt_sstore.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<Tuple> tuples = new ArrayDeque<>();

    public ConcurrentHashMap<String, ConcurrentSkipListMap<Long, SchemaRecord>> windowMap = new ConcurrentHashMap<>();

    public GSWBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public GSWBolt_sstore(int fid) {
        super(LOG, fid, null);

    }


    @Override
    protected boolean READ_CORE(WindowedMicroEvent event) {
        long sum = 0;

        // apply function
        AppConfig.randomDelay();

        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
            SchemaRecordRef ref = event.getRecord_refs()[i];
            if (ref.isEmpty()) {
                return false;//not yet processed.
            }

            DataBox keyBox = ref.getRecord().getValues().get(0);
            List<SchemaRecord> schemaRecordRange = readPreValuesRange(windowMap.computeIfAbsent(
                            keyBox.getString().trim(), k -> new ConcurrentSkipListMap<>()),
                    event.getBid(), AppConfig.windowSize);
            sum += schemaRecordRange.stream().mapToLong(schemaRecord -> schemaRecord.getValues().get(1).getLong()).sum();


            DataBox dataBox = ref.getRecord().getValues().get(1);
            long read_result = Long.parseLong(dataBox.getString().trim());
            event.result.add(read_result);
        }
        return true;
    }


    public List<SchemaRecord> readPreValuesRange(ConcurrentSkipListMap<Long, SchemaRecord> window, long ts, long range) {
        long start = ts - range < 0 ? 0 : ts - range;
        ConcurrentNavigableMap<Long, SchemaRecord> schemaRange = window.tailMap(start);

        //not modified in last round
        if (schemaRange.size() == 0)
            System.out.println("Empty window");
        else
            System.out.println(schemaRange.size());

        return new ArrayList<>(schemaRange.values());
    }

    @Override
    protected void WRITE_CORE(WindowedMicroEvent event) {
//        long start = System.nanoTime();
        long sum = 0;
        SchemaRecordRef ref = event.getRecord_refs()[0];

        // insert the write records into the window state
        DataBox keyBox = ref.getRecord().getValues().get(0);
        ConcurrentSkipListMap<Long, SchemaRecord> curWindow = windowMap.computeIfAbsent(keyBox.getString().trim(), k -> new ConcurrentSkipListMap<>());
        curWindow.put(event.getBid(), ref.getRecord());

        DataBox TargetValue_value = ref.getRecord().getValues().get(1);

        int NUM_ACCESS = event.TOTAL_NUM_ACCESS / event.Txn_Length;
        for (int j = 0; j < event.Txn_Length; ++j) {
            AppConfig.randomDelay();
            for (int i = 0; i < NUM_ACCESS; ++i) {
                int offset = j * NUM_ACCESS + i;
                SchemaRecordRef recordRef = event.getRecord_refs()[offset];
                SchemaRecord record = recordRef.getRecord();
                DataBox Value_value = record.getValues().get(1);
                final long Value = Value_value.getLong();
                sum += Value;
            }
        }
        sum /= event.TOTAL_NUM_ACCESS;
        TargetValue_value.setLong(sum);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks());
        if (!enable_states_partition) {
            if (enable_log) LOG.info("Please enable `enable_states_partition` for PAT scheme");
            System.exit(-1);
        }
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
//        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().GetAndUpdate(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock, this.context.getNUMTasks());
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

    public void start_evaluate(int thread_Id, long mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        SOURCE_CONTROL.getInstance().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        // add bid_array for events
        if (thread_Id == 0) {
            int partitionOffset = config.getInt("NUM_ITEMS") / tthread;
            int[] p_bids = new int[(int) tthread];
            HashMap<Integer, Integer> pids = new HashMap<>();
            for (TxnEvent event : GlobalSorter.sortedEvents) {
                if (event instanceof MicroEvent) {
                    parseWindowMicroEvent(partitionOffset, (MicroEvent) event, pids);
                    event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                    pids.replaceAll((k, v) -> p_bids[k]++);
                } else if (event instanceof WindowedMicroEvent) {
                    parseWindowMicroEvent(partitionOffset, (WindowedMicroEvent) event, pids);
                    event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                    pids.replaceAll((k, v) -> p_bids[k]++);
                } else {
                    throw new UnsupportedOperationException();
                }
                pids.clear();
            }
            GlobalSorter.sortedEvents.clear();
        }
        SOURCE_CONTROL.getInstance().postStateAccessBarrier(thread_Id);
    }

    private void parseWindowMicroEvent(int partitionOffset, MicroEvent event, HashMap<Integer, Integer> pids) {
        for (int key : event.getKeys()) {
            pids.put(key / partitionOffset, 0);
        }
    }

    private void parseWindowMicroEvent(int partitionOffset, WindowedMicroEvent event, HashMap<Integer, Integer> pids) {
        for (int key : event.getKeys()) {
            pids.put(key / partitionOffset, 0);
        }
    }

    @Override
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
//            System.out.println("thread: "+thread_Id+", event_id: "+_bid);
            TxnEvent event = (TxnEvent) input_event;
            GlobalSorter.addEvent(event);
        }
    }

    @Override
    protected void LAL_PROCESS(long _bid) throws DatabaseException {
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        WindowedMicroEvent event = (WindowedMicroEvent) input_event;
        int _pid = event.getPid();
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        LA_LOCK_Reentrance(transactionManager, event.getBid_array(), event.partition_indexs, _bid, thread_Id);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        LAL(event, _bid, _bid);
        END_LOCK_TIME_MEASURE_ACC(thread_Id);
        LA_UNLOCK_Reentrance(transactionManager, event.partition_indexs, thread_Id);
        END_WAIT_TIME_MEASURE_ACC(thread_Id);
    }
}
