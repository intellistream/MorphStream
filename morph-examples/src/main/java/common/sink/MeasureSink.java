package common.sink;

import common.Constants;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.datatype.util.LRTopologyControl;
import common.sink.helper.stable_sink_helper;
import engine.stream.components.operators.api.BaseSink;
import engine.stream.execution.ExecutionGraph;
import engine.stream.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.profiler.MeasureTools;
import util.AppConfig;
import engine.txn.utils.SINK_CONTROL;

import java.io.*;
import java.util.ArrayDeque;
import java.util.HashMap;

import static common.CONTROL.*;
import static engine.IRunner.CCOption_SStore;

public class MeasureSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(MeasureSink.class);
    private DescriptiveStatistics latency = new DescriptiveStatistics();
    private static final long serialVersionUID = 6249684803036342603L;
    protected final ArrayDeque<Long> latency_map = new ArrayDeque();
    public int punctuation_interval;
    protected stable_sink_helper helper;
    protected int ccOption;
    public int tthread;
    int cnt = 0;
    long start;
    public int totalEvents;
    public double interval;
    public long previous_measure_time;
    public boolean isRecovery;
    public boolean isFailure;
    public long remainTime = 0;

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
        interval = config.getDouble("measureInterval");
        isRecovery = config.getBoolean("isRecovery");
        isFailure = config.getBoolean("isFailure");
        if (isRecovery) {
            remainTime = (long) (config.getInt("failureTime") * 1E6);
        }
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
        if (thisTaskId == 0)
            setMetricDirectory(config);
        SINK_CONTROL.getInstance().config();
        if (enable_log) LOG.info("expected last events = " + totalEvents);
    }

    @Override
    public void execute(Tuple input) throws InterruptedException {
        check(cnt, input);
        cnt ++;
    }

    protected void latency_measure(Tuple input) {
        cnt ++;
        if (enable_latency_measurement) {
            this.latency.addValue(System.nanoTime() - input.getLong(1));
            long interval = System.nanoTime() - previous_measure_time;
            if (interval / 1E6 >= this.interval) {
                MeasureTools.THROUGHPUT_MEASURE(this.thisTaskId, cnt, interval / 1E6);
                MeasureTools.LATENCY_MEASURE(this.thisTaskId, this.latency.getMean() / 1E6);
                this.latency = new DescriptiveStatistics();
                previous_measure_time = System.nanoTime();
            }
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
     * @param results
     */
    protected void measure_end(double results) {
        if (enable_log) LOG.info(Thread.currentThread().getName() + " obtains lock");
        SINK_CONTROL.getInstance().throughput = results;
        if (enable_log) LOG.info("Thread:" + thisTaskId + " is going to stop all threads sequentially");
        context.Sequential_stopAll();
        SINK_CONTROL.getInstance().unlock();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    public void setMetricDirectory(Configuration config) {
        String fileName;
        if (isRecovery) {
            fileName = "_recovery";
        } else if (isFailure) {
            fileName = "_failure";
        } else {
            fileName = "";
        }
        String statsFolderPattern = OsUtils.osWrapperPostFix(config.getString("rootFilePath"))
                + OsUtils.osWrapperPostFix("stats")
                + OsUtils.osWrapperPostFix("%s")
                + OsUtils.osWrapperPostFix("%s")
                + OsUtils.osWrapperPostFix("FTOption = %d")
                + OsUtils.osWrapperPostFix("threads = %d")
                + OsUtils.osWrapperPostFix("totalEvents = %d")
                + ("%d_%d_%d_%d_%d_%d_%s_%s_%d_%s_%s_%s_%s_%d");

        String scheduler = config.getString("scheduler");
        if (config.getInt("CCOption") == CCOption_SStore) {
            scheduler = "PAT";
        }
        String directory;
        // TODO: to be refactored
        if (config.getString("common").equals("StreamLedger")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler,
                    config.getInt("FTOption"),
                    tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("Ratio_Of_Deposit"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getString("compressionAlg"),
                    config.getInt("complexity"),
                    config.getBoolean("isHistoryView"),
                    config.getBoolean("isAbortPushDown"),
                    config.getBoolean("isTaskPlacing"),
                    config.getBoolean("isSelectiveLogging"),
                    config.getInt("checkpoint"));
        } else if (config.getString("common").equals("GrepSum")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler,
                    config.getInt("FTOption"),
                    tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("Transaction_Length"),
                    config.getInt("Ratio_of_Multiple_State_Access"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    AppConfig.isCyclic,
                    config.getString("compressionAlg"),
                    config.getInt("complexity"),
                    config.getBoolean("isHistoryView"),
                    config.getBoolean("isAbortPushDown"),
                    config.getBoolean("isTaskPlacing"),
                    config.getBoolean("isSelectiveLogging"),
                    config.getInt("checkpoint"));
        } else if (config.getString("common").equals("OnlineBiding")){
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler,
                    config.getInt("FTOption"),
                    tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getString("compressionAlg"),
                    config.getInt("complexity"),
                    config.getBoolean("isHistoryView"),
                    config.getBoolean("isAbortPushDown"),
                    config.getBoolean("isTaskPlacing"),
                    config.getBoolean("isSelectiveLogging"),
                    config.getInt("checkpoint"));
        } else if (config.getString("common").equals("TollProcessing")) {
            directory = String.format(statsFolderPattern,
                    config.getString("common"), scheduler,
                    config.getInt("FTOption"),
                    tthread, totalEvents,
                    config.getInt("NUM_ITEMS"),
                    config.getInt("NUM_ACCESS"),
                    config.getInt("State_Access_Skewness"),
                    config.getInt("Ratio_of_Overlapped_Keys"),
                    config.getInt("Ratio_of_Transaction_Aborts"),
                    config.getInt("Transaction_Length"),
                    AppConfig.isCyclic,
                    config.getString("compressionAlg"),
                    config.getInt("complexity"),
                    config.getBoolean("isHistoryView"),
                    config.getBoolean("isAbortPushDown"),
                    config.getBoolean("isTaskPlacing"),
                    config.getBoolean("isSelectiveLogging"),
                    config.getInt("checkpoint"));
        } else if (config.getString("common").equals("WindowedGrepSum")) {
                directory = String.format(statsFolderPattern,
                        config.getString("common"), scheduler,
                        config.getInt("FTOption"),
                        tthread, totalEvents,
                        config.getInt("NUM_ITEMS"),
                        config.getInt("Ratio_of_Multiple_State_Access"),
                        config.getInt("State_Access_Skewness"),
                        config.getInt("Period_of_Window_Reads"),
                        config.getInt("windowSize"),
                        config.getInt("Transaction_Length"),
                        AppConfig.isCyclic,
                        config.getString("compressionAlg"),
                        config.getInt("complexity"),
                        config.getBoolean("isHistoryView"),
                        config.getBoolean("isAbortPushDown"),
                        config.getBoolean("isTaskPlacing"),
                        config.getBoolean("isSelectiveLogging"),
                        config.getInt("checkpoint"));
            } else if (config.getString("common").equals("SHJ")) {
                directory = String.format(statsFolderPattern,
                        config.getString("common"), scheduler,
                        config.getInt("FTOption"),
                        tthread, totalEvents,
                        config.getInt("NUM_ITEMS"),
                        config.getInt("Ratio_of_Multiple_State_Access"),
                        config.getInt("State_Access_Skewness"),
                        config.getInt("Ratio_of_Overlapped_Keys"),
                        config.getInt("Ratio_of_Transaction_Aborts"),
                        config.getInt("Transaction_Length"),
                        AppConfig.isCyclic,
                        config.getString("compressionAlg"),
                        config.getInt("complexity"),
                        config.getBoolean("isHistoryView"),
                        config.getBoolean("isAbortPushDown"),
                        config.getBoolean("isTaskPlacing"),
                        config.getBoolean("isSelectiveLogging"),
                        config.getInt("checkpoint"));
            } else if (config.getString("common").equals("NonGrepSum")) {
                directory = String.format(statsFolderPattern,
                        config.getString("common"), scheduler,
                        config.getInt("FTOption"),
                        tthread, totalEvents,
                        config.getInt("NUM_ITEMS"),
                        config.getInt("NUM_ACCESS"),
                        config.getInt("State_Access_Skewness"),
                        config.getInt("Ratio_of_Non_Deterministic_State_Access"),
                        config.getInt("Ratio_of_Transaction_Aborts"),
                        config.getInt("Transaction_Length"),
                        AppConfig.isCyclic,
                        config.getString("compressionAlg"),
                        config.getInt("complexity"),
                        config.getBoolean("isHistoryView"),
                        config.getBoolean("isAbortPushDown"),
                        config.getBoolean("isTaskPlacing"),
                        config.getBoolean("isSelectiveLogging"),
                        config.getInt("checkpoint"));
            } else {
            throw new UnsupportedOperationException();
        }
        File file = new File(directory + fileName + ".runtime");
        file.mkdirs();
        if (file.exists()) {
            try {
                file.delete();
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        MeasureTools.setMetricDirectory(directory);
        MeasureTools.setMetricFileNameSuffix(fileName);
    }
}
