package intellistream.morphstream.examples.utils;

import intellistream.morphstream.api.input.TransactionalEvent;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.delete.TransactionalBolt;
import intellistream.morphstream.engine.stream.components.operators.api.delete.TransactionalSpout;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.api.input.InputSource;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public abstract class SPOUTCombo extends TransactionalSpout {
    private static final long serialVersionUID = -2394340130331865581L;
    private static Logger LOG;
    public final String split_exp = ";";
    public int the_end;
    public int global_cnt;
    public int num_events_per_thread;
    public long[] mybids;
    public Object[] myevents;
    public int counter;
    public Tuple tuple;
    public Tuple marker;
    public GeneralMsg generalMsg;
    public int tthread;
    public SINKCombo sink = new SINKCombo();
    protected int totalEventsPerBatch = 0;
    protected TransactionalBolt bolt;//compose the bolt here.
    int start_measure;
    Random random = new Random();

    public SPOUTCombo(Logger log, int i) {
        super(log, i);
        LOG = log;
        this.scalable = false;
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

                //TODO: InputSource inputSource should be retrieved from client.getInputSource();
                //TODO: Should we keep both streaming and original input?

                Object event = myevents[counter]; //this should be txnEvent already

                long bid = mybids[counter];
                if (CONTROL.enable_latency_measurement)
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, System.nanoTime());
                else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
                }

                tuple = new Tuple(bid, this.taskId, context, generalMsg);
                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
                counter++;

                if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                    if (model_switch(counter)) {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration));
                        bolt.execute(marker);
                    }
                }

                if (counter == the_end) { //TODO: Refactor this part, remove the_end, use stopEvent indicator
//                    if (ccOption == CCOption_SStore)
//                        MeasureTools.END_TOTAL_TIME_MEASURE(taskId);//otherwise deadlock.
                    SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                    SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                    if (taskId == 0)
                        sink.end(global_cnt);
                }
            }
        } catch (DatabaseException | BrokenBarrierException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;//4 for 7 sockets
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
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


        punctuation_interval = config.getInt("checkpoint");
        // setup the checkpoint interval for measurement
        sink.punctuation_interval = punctuation_interval;

        target_Hz = (int) config.getDouble("targetHz", 10000000);

        totalEventsPerBatch = config.getInt("totalEvents");
        tthread = config.getInt("tthread");

        num_events_per_thread = totalEventsPerBatch / tthread;

        if (enable_log) LOG.info("total events... " + totalEventsPerBatch);
        if (enable_log) LOG.info("total events per thread = " + num_events_per_thread);
        if (enable_log) LOG.info("checkpoint_interval = " + punctuation_interval);

        start_measure = CONTROL.MeasureStart;

        mybids = new long[num_events_per_thread];
        myevents = new Object[num_events_per_thread];
        the_end = num_events_per_thread;

        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            global_cnt = (the_end) * tthread;
        } else {
            global_cnt = (the_end - CONTROL.MeasureStart) * tthread;
        }
    }
    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        bolt.loadDB(conf, context, collector);
    }
}