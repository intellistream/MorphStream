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
import utils.AppConfig;
import utils.SINK_CONTROL;

import java.io.*;
import java.util.ArrayDeque;
import java.util.HashMap;

import static common.CONTROL.*;
import static common.IRunner.CCOption_LOCK;
import static common.IRunner.CCOption_SStore;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private static final DescriptiveStatistics latency = new DescriptiveStatistics();
    private static final long serialVersionUID = 6249684803036342603L;
    protected static String directory;
    protected final ArrayDeque<Long> latency_map = new ArrayDeque();
    public int checkpoint_interval;
    public int tthread;
    public int totalEvents;
    protected stable_sink_helper helper;
    protected int ccOption;
    int cnt = 0;
    long start;

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

        totalEvents = config.getInt("totalEvents");
        tthread = config.getInt("tthread");

        String statsFolderPattern = OsUtils.osWrapperPostFix(config.getString("rootFilePath"))
                + OsUtils.osWrapperPostFix("stats")
                + OsUtils.osWrapperPostFix("%s")
                + OsUtils.osWrapperPostFix("%s")
                + OsUtils.osWrapperPostFix("threads = %d")
                + OsUtils.osWrapperPostFix("totalEvents = %d")
                + OsUtils.osWrapperPostFix("%d_%d_%d_%d_%d_%d_%s_%d.latency");

        String scheduler = config.getString("scheduler");
        if (config.getInt("CCOption") == CCOption_SStore) {
            scheduler = "PAT";
        } else if (config.getInt("CCOption") == CCOption_LOCK) {
            scheduler = "NOCC";
        }

        // TODO: to be refactored
        if (config.getString("common").equals("StreamLedger")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("Ratio_Of_Deposit"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else if (config.getString("common").equals("GrepSum")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else if (config.getString("common").equals("OnlineBiding")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else if (config.getString("common").equals("TollProcessing")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else if (config.getString("common").equals("EventDetection")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else if (config.getString("common").equals("EventDetectionSliding")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else if (config.getString("common").equals("IBWJ")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else if (config.getString("common").equals("LoadBalancer")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler, tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getInt("complexity"));
        } else {
            throw new UnsupportedOperationException();
        }
        File file = new File(directory);
        file.mkdirs();

        if (file.exists())
            file.delete();
        try {
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }

        SINK_CONTROL.getInstance().config();
        tthread = this.config.getInt("tthread");
        totalEvents = config.getInt("totalEvents");
        if (enable_log) LOG.info("expected last events = " + totalEvents);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        check(cnt, input);
        cnt++;
        LOG.info("Sink received tuple: " + cnt);
    }

    public void execute() throws InterruptedException {}

    protected void latency_measure(Tuple input) {
        cnt++;
        if (enable_latency_measurement) {
//            LOG.info("Fix Me.");
//            if (cnt == 1) {
//                start = System.nanoTime();
//            } else {
//                if (cnt % checkpoint_interval == 0) {
//                    final long end = System.nanoTime();
//                    final long process_latency = end - start;//ns
//                    latency_map.add(process_latency / checkpoint_interval);
//                    start = end;
//                }
//            }
            latency_map.add(System.nanoTime() - input.getLong(1));
        }
    }

    protected void check(int cnt, Tuple input) {
        if (cnt == 0) {
            helper.StartMeasurement();
        } else if (cnt == (totalEvents - 40 * 10 - 1)) {
            double results = helper.EndMeasurement(cnt);
            this.setResults(results);
            if (!enable_engine)//performance measure for TStream is different.
                if (enable_log) LOG.info("Received:" + cnt + " throughput:" + results);
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
        if (enable_log) LOG.info(Thread.currentThread().getName() + " obtains lock");
        if (enable_latency_measurement) {
            StringBuilder sb = new StringBuilder();
            for (Long entry : latency_map) {
                latency.addValue((entry / 1E6));
            }
            try {
                FileWriter f = null;
                f = new FileWriter(new File(directory));
                Writer w = new BufferedWriter(f);
//                for (double percentile = 0.5; percentile <= 100.0; percentile += 0.5) {
//                    w.write(latency.getPercentile(percentile) + "\n");
//                }
                for (double lat : latency.getValues()) {
                    w.write(lat + "\n");
                }
                sb.append("=======Details=======");
                sb.append("\n" + latency.toString() + "\n");
                sb.append("===99th===" + "\n");
                sb.append(latency.getPercentile(99) + "\n");
                w.write(sb.toString());
                w.write("Percentile\t Latency\n");
                w.write(String.format("%f\t" +
                                "%-10.4f\t"
                        , 0.5, latency.getPercentile(0.5)) + "\n");
                for (double i = 20; i < 100; i += 20) {
                    String output = String.format("%f\t" +
                                    "%-10.4f\t"
                            , i, latency.getPercentile(i));
                    w.write(output + "\n");
                }
                w.write(String.format("%d\t" +
                                "%-10.4f\t"
                        , 99, latency.getPercentile(99)));
                w.close();
                f.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (enable_log) LOG.info(sb.toString());
        }
        SINK_CONTROL.getInstance().throughput = results;
        if (enable_log) LOG.info("Thread:" + thisTaskId + " is going to stop all threads sequentially");
        context.Sequential_stopAll();
        SINK_CONTROL.getInstance().unlock();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
}
