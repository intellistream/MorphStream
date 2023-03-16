package common.spout;

import benchmark.DataHolder;
import combo.SINKCombo;
import common.CONTROL;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import components.operators.api.TransactionalSpout;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_log;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.Content.CCOption_SStore;

public class EDSSpout extends TransactionalSpout {

    private static final Logger LOG = LoggerFactory.getLogger(EDSSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;

    public final String split_exp = ";";
    public int the_end;
    public int global_cnt;
    public double[] mybids;
    public Object[] myevents;
    public int counter;
    public Tuple tuple;
    public Tuple marker;
    public GeneralMsg generalMsg;
    public int tthread;
    public SINKCombo sink = new SINKCombo();
    protected int totalEventsPerBatch = 0;


    int start_measure;

    public EDSSpout() {
        super(LOG, 0);
        this.scalable = false;
    }

    public EDSSpout(Logger log, int i) {
        super(log, i);
        this.scalable = false;
    }

    public void loadEvent() {
        int storageIndex = 0;
        //Load Transfer Events.
        for (int index = taskId; index < DataHolder.events.size(); ) {
            TxnEvent event = DataHolder.events.get(index).cloneEvent();
            mybids[storageIndex] = event.getBid();
            myevents[storageIndex++] = event;
            if (storageIndex == totalEventsPerBatch)
                break;
            index += combo_bid_size;
        }
        assert (storageIndex == totalEventsPerBatch);
    }

    @Override
    public void nextTuple() throws InterruptedException {
        if (counter == start_measure) {
            if (taskId == 0)
                sink.start();
        }
        if (counter < totalEventsPerBatch) {
            Object event = myevents[counter];

            double bid = mybids[counter];
            if (CONTROL.enable_latency_measurement)
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
            else {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
            }

            tuple = new Tuple(bid, this.taskId, context, generalMsg);
            this.collector.emit(bid, tuple);
            counter++;
        }
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;//4 for 7 sockets
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        if (enable_log) LOG.info("Spout initialize is being called");
        long start = System.nanoTime();
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        long pid = OsUtils.getPID();
        if (enable_log) LOG.info("JVM PID  = " + pid);
        long end = System.nanoTime();
        if (enable_log) LOG.info("spout initialize takes (ms):" + (end - start) / 1E6);
        ccOption = config.getInt("CCOption", 0);
        bid = 0;
        counter = 0;

        checkpoint_interval = config.getInt("checkpoint");
        // setup the checkpoint interval for measurement
        sink.checkpoint_interval = checkpoint_interval;

        target_Hz = (int) config.getDouble("targetHz", 10000000);

        tthread = config.getInt("tthread");
        totalEventsPerBatch = config.getInt("totalEvents") + tthread; //Add stopping signals

        if (enable_log) LOG.info("total events... " + totalEventsPerBatch);
        if (enable_log) LOG.info("checkpoint_interval = " + checkpoint_interval);

        start_measure = CONTROL.MeasureStart;

        mybids = new double[totalEventsPerBatch];
        myevents = new Object[totalEventsPerBatch];
        the_end = totalEventsPerBatch;

        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            global_cnt = (the_end) * tthread;
        } else {
            global_cnt = (the_end - CONTROL.MeasureStart) * tthread;
        }

        sink.configPrefix = this.getConfigPrefix();
        sink.prepare(config, context, collector);
        _combo_bid_size = combo_bid_size;

        loadEvent();
    }

}
