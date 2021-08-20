package common.sink;

import common.Constants;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.datatype.util.LRTopologyControl;
import common.sink.helper.stable_sink_helper;
import components.operators.api.BaseSink;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.SINK_CONTROL;

import java.io.*;
import java.util.ArrayDeque;
import java.util.HashMap;

import static common.CONTROL.*;
import static common.Constants.STAT_Path;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private static final DescriptiveStatistics latency = new DescriptiveStatistics();
    private static final long serialVersionUID = 6249684803036342603L;
    protected static String directory;


    protected final ArrayDeque<Long> latency_map = new ArrayDeque();
    public int batch_number_per_wm;
    protected stable_sink_helper helper;
    protected int ccOption;
    protected int tthread;
    int cnt = 0;
    long start;

    private int exe;

    public MeasureSink() {
        super(new HashMap<>());
        this.input_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        this.input_selectivity.put(LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID, 1.0);
    }

    @Override
    public Integer default_scale(Configuration conf) {
        int numNodes = conf.getInt("num_socket", 1);
        return 1;
    }

    public void initialize(int task_Id_InGroup, int thisTaskId, ExecutionGraph graph) {
        super.initialize(task_Id_InGroup, thisTaskId, graph);
        int size = graph.getSink().operator.getExecutorList().size();
        ccOption = config.getInt("CCOption", 0);
        String path = config.getString("metrics.output");
        helper = new stable_sink_helper(LOG
                , config.getInt("runtimeInSeconds")
                , path
                , config.getDouble("predict", 0)
                , size
                , thisTaskId
                , config.getBoolean("measure", false));

        directory = STAT_Path + OsUtils.OS_wrapper("TstreamPlus")
                + OsUtils.OS_wrapper(configPrefix)
                + OsUtils.OS_wrapper(String.valueOf(config.getDouble("checkpoint")));
        File file = new File(directory);
        if (!file.mkdirs()) {
        }
        SINK_CONTROL.getInstance().config();
        tthread = this.config.getInt("tthread");
        exe = config.getInt("totalEventsPerBatch") * config.getInt("numberOfBatches");
        if(enable_log) LOG.info("expected last events = " + exe);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        check(cnt, input);
        cnt++;
    }

    protected void latency_measure(Tuple input) {
        if (enable_latency_measurement) {
            if (cnt == 0) {
                start = System.nanoTime();
            } else {
                if (cnt % batch_number_per_wm == 0) {
                    final long end = System.nanoTime();
                    final long process_latency = end - start;//ns
                    latency_map.add(process_latency / batch_number_per_wm);
                    start = end;
                }
            }
            cnt++;
        }
    }

    protected void check(int cnt, Tuple input) {
        if (cnt == 0) {
            helper.StartMeasurement();
        } else if (cnt == (exe - 40 * 10 - 1)) {
            double results = helper.EndMeasurement(cnt);
            this.setResults(results);
            if (!enable_engine)//performance measure for TStream is different.
                if(enable_log) LOG.info("Received:" + cnt + " throughput:" + results);
            if (thisTaskId == graph.getSink().getExecutorID()) {
                measure_end(results);
            }
        }
    }

    /**
     * Only one sink will do the measure_end.
     *
     * @param results
     */
    protected void measure_end(double results) {
        if(enable_log) LOG.info(Thread.currentThread().getName() + " obtains lock");
        if (enable_latency_measurement) {
            StringBuilder sb = new StringBuilder();
            for (Long entry : latency_map) {
                latency.addValue((entry / 1E6));
            }
            try {
                FileWriter f = null;
                f = new FileWriter(new File(directory
                        + OsUtils.OS_wrapper(ccOption + ".latency")));
                Writer w = new BufferedWriter(f);
                for (double percentile = 0.5; percentile <= 100.0; percentile += 0.5) {
                    w.write(latency.getPercentile(percentile) + "\n");
                }
                sb.append("=======Details=======");
                sb.append("\n" + latency.toString() + "\n");
                sb.append("===99th===" + "\n");
                sb.append(latency.getPercentile(99) + "\n");
                w.write(sb.toString());
                w.close();
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(enable_log) LOG.info(sb.toString());
        }
        SINK_CONTROL.getInstance().throughput = results;
        if(enable_log) LOG.info("Thread:" + thisTaskId + " is going to stop all threads sequentially");
        context.Sequential_stopAll();
        SINK_CONTROL.getInstance().unlock();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
