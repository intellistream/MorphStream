package common.combo;

import common.CONTROL;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.sink.SINKCombo;
import common.tools.FastZipfGenerator;
import components.context.TopologyContext;
import components.operators.api.TransactionalBolt;
import components.operators.api.TransactionalSpout;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import profiler.MeasureTools;
import utils.SOURCE_CONTROL;

import java.io.FileNotFoundException;
import java.util.concurrent.BrokenBarrierException;

import static common.Constants.DEFAULT_STREAM_ID;
import static content.Content.CCOption_SStore;
import static content.Content.CCOption_TStream;
import static profiler.Metrics.NUM_ITEMS;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public abstract class SPOUTCombo extends TransactionalSpout {
    private static final long serialVersionUID = -2394340130331865581L;
    private static Logger LOG;
    public final String split_exp = ";";
    public int the_end;
    public int global_cnt;
    public int num_events_per_thread;
    public int test_num_events_per_thread;
    public long[] mybids;
    public Object[] myevents;
    public int counter;
    public Tuple tuple;
    public Tuple marker;
    public GeneralMsg generalMsg;
    public int tthread;
    public SINKCombo sink = new SINKCombo();
    protected int totalEventsPerBatch = 0;
    protected int numberOfBatches = 0;
    TransactionalBolt bolt;//compose the bolt here.
    int start_measure;

//    event = new TransactionEvent(
//                    0, //bid
//                            0, //pid
//                            "[0]", //bid_array
//                            1,//num_of_partition
//                            "0",//getSourceAccountId
//                            "0",//getSourceBookEntryId
//                            "1",//getTargetAccountId
//                            "1",//getTargetBookEntryId
//                            2,  //getAccountTransfer
//                            2  //getBookEntryTransfer
//    );

    public SPOUTCombo(Logger log, int i) {
        super(log, i);
        LOG = log;
        this.scalable = false;
        state = new ValueState();

    }

    public abstract void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) throws FileNotFoundException;

    @Override
    public void nextTuple() throws InterruptedException {
        try {
            if (counter == start_measure) {
                if (taskId == 0)
                    sink.start();
            }
            if (counter < num_events_per_thread) {

                Object event = myevents[counter];
//                if(event==null){
//                    LOG.error("?");
//                }
                long bid = mybids[counter];
                if (CONTROL.enable_latency_measurement)
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
                else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
                }

                tuple = new Tuple(bid, this.taskId, context, generalMsg);
                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter++;

                if (ccOption == CCOption_TStream) {// This is only required by T-Stream.
                    if (!CONTROL.enable_app_combo) {
                        forward_checkpoint(this.taskId, bid, null);
                    } else {
                        if (checkpoint(counter)) {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration));
                            bolt.execute(marker);
                        }
                    }
                }

                if (counter == the_end) {
                    if (ccOption == CCOption_SStore)
                        MeasureTools.END_TOTAL_TIME_MEASURE(taskId);//otherwise deadlock.
                    SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                    if (taskId == 0)
                        sink.end(global_cnt);
                }
            }
        } catch (DatabaseException | BrokenBarrierException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;//4 for 7 sockets
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("Spout initialize is being called");
        long start = System.nanoTime();
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        long pid = OsUtils.getPID();
        LOG.info("JVM PID  = " + pid);
        long end = System.nanoTime();
        LOG.info("spout initialize takes (ms):" + (end - start) / 1E6);
        ccOption = config.getInt("CCOption", 0);
        bid = 0;
        counter = 0;
        tthread = config.getInt("tthread");
        totalEventsPerBatch = config.getInt("totalEventsPerBatch");
        numberOfBatches = config.getInt("numberOfBatches");

        checkpoint_interval_sec = config.getDouble("checkpoint");
        target_Hz = (int) config.getDouble("targetHz", 10000000);
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 0);
        p_generator = new FastZipfGenerator(NUM_ITEMS, theta, 0);

        batch_number_per_wm = totalEventsPerBatch / tthread;//10K, 1K, 100.

        if (batch_number_per_wm * tthread < totalEventsPerBatch) // assuming combo_bid_size == 1 always
            if (thread_Id < (totalEventsPerBatch - batch_number_per_wm * tthread))
                batch_number_per_wm++;

        num_events_per_thread = batch_number_per_wm * numberOfBatches;

        LOG.info("total events per batch... " + totalEventsPerBatch);
        LOG.info("events per thread... " + num_events_per_thread);
        LOG.info("batch_number_per_wm (watermark events length)= " + batch_number_per_wm);

        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            test_num_events_per_thread = num_events_per_thread;//otherwise deadlock.. TODO: fix it later.
            start_measure = 0;
        } else {
            test_num_events_per_thread = num_events_per_thread;//otherwise deadlock.. TODO: fix it later.
            start_measure = CONTROL.MeasureStart;
        }

        mybids = new long[num_events_per_thread];//5000 batches.
        myevents = new Object[num_events_per_thread];
        the_end = num_events_per_thread;

        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            global_cnt = (the_end) * tthread;
        } else {
            global_cnt = (the_end - CONTROL.MeasureStart) * tthread;
        }
    }
}