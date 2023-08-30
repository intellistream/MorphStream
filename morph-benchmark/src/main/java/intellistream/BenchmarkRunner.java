package intellistream;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import intellistream.morphstream.common.constants.BaseConstants;
import intellistream.morphstream.common.constants.GrepSumConstants;
import intellistream.morphstream.common.constants.NonGrepSumConstants;
import intellistream.morphstream.common.constants.SHJConstants;
import intellistream.morphstream.common.io.Enums.platform.HP_Machine;
import intellistream.morphstream.common.io.Enums.platform.HUAWEI_Machine;
import intellistream.morphstream.common.io.Enums.platform.Platform;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.TopologyComponent;
import intellistream.morphstream.engine.stream.components.exception.UnhandledCaseException;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.executorThread;
import intellistream.morphstream.engine.stream.topology.TopologySubmitter;
import intellistream.morphstream.engine.txn.durability.struct.FaultToleranceRelax;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.profiler.Metrics;
import intellistream.morphstream.engine.txn.scheduler.struct.OperationChainCommon;
import intellistream.morphstream.engine.txn.utils.SINK_CONTROL;
import intellistream.morphstream.examples.streaming.WordCount;
import intellistream.morphstream.examples.tsp.grepsum.GrepSum;
import intellistream.morphstream.examples.tsp.grepsumnon.NonGrepSum;
import intellistream.morphstream.examples.tsp.grepsumwindow.WindowedGrepSum;
import intellistream.morphstream.examples.tsp.onlinebiding.OnlineBiding;
import intellistream.morphstream.examples.tsp.shj.SHJ;
import intellistream.morphstream.examples.tsp.streamledger.StreamLedger;
import intellistream.morphstream.examples.tsp.tollprocessing.TollProcessing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.common.constants.LinearRoadConstants.Conf.Executor_Threads;
import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
import static intellistream.morphstream.common.constants.StreamLedgerConstants.Conf.SL_THREADS;
import static intellistream.morphstream.configuration.CONTROL.*;
import static intellistream.morphstream.engine.txn.content.LVTStreamContent.LVTSTREAM_CONTENT;
import static intellistream.morphstream.engine.txn.content.LWMContentImpl.LWM_CONTENT;
import static intellistream.morphstream.engine.txn.content.LockContentImpl.LOCK_CONTENT;
import static intellistream.morphstream.engine.txn.content.SStoreContentImpl.SSTORE_CONTENT;
import static intellistream.morphstream.engine.txn.content.TStreamContentImpl.T_STREAMCONTENT;
import static intellistream.morphstream.engine.txn.content.common.ContentCommon.content_type;
import static intellistream.morphstream.engine.txn.content.common.ContentCommon.loggingRecord_type;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.METRICS_REPORT;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.METRICS_REPORT_WITH_FAILURE;
import static intellistream.morphstream.engine.txn.profiler.Metrics.timer;
import static intellistream.morphstream.util.FaultToleranceConstants.*;

public class BenchmarkRunner extends Runner {
    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);
    private static Topology final_topology;
    private final AppDriver driver;
    private final Configuration config = new Configuration();
    private Platform platform;

    public BenchmarkRunner() {
        driver = new AppDriver();
        //Ordinary Application
        driver.addApp("WordCount", WordCount.class);//WC

        //Transactional Application
        driver.addApp("GrepSum", GrepSum.class);//GS
        driver.addApp("NonGrepSum", NonGrepSum.class);//NonGS
        driver.addApp("WindowedGrepSum", WindowedGrepSum.class);//G
        driver.addApp("StreamLedger", StreamLedger.class);//SL
        driver.addApp("OnlineBiding", OnlineBiding.class);//OB
        driver.addApp("TollProcessing", TollProcessing.class);//TP
        driver.addApp("SHJ", SHJ.class);//G
    }

    public static void main(String[] args) {
        if (enable_log) log.info("Program Starts..");
        BenchmarkRunner runner = new BenchmarkRunner();
        JCommander cmd = new JCommander(runner);
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            if (enable_log) log.error("Argument error: " + ex.getMessage());
            cmd.usage();
        }
        try {
            runner.run();
        } catch (InterruptedException ex) {
            if (enable_log) log.error("Error in running topology locally", ex);
        }
    }

    private static double runTopologyLocally(Topology topology, Configuration conf) throws InterruptedException {
        if (enable_memory_measurement) {
            timer.scheduleAtFixedRate(new Metrics.RuntimeHardware(conf.getString("rootFilePath")), 0, 10);
        }
        TopologySubmitter submitter = new TopologySubmitter();
        try {
            final_topology = submitter.submitTopology(topology, conf);
        } catch (UnhandledCaseException e) {
            e.printStackTrace();
        }
        executorThread sinkThread = submitter.getOM().getEM().getSinkThread();
        long start = System.currentTimeMillis();
        if (conf.getBoolean("isFailure")) {
            int failureTime = conf.getInt("failureTime"); // Emulate system failure after (ms)
            sinkThread.join(failureTime);

            if (enable_log) log.info("System failure after " + failureTime / 1E3 + "s.");
            METRICS_REPORT_WITH_FAILURE(conf.getInt("CCOption", 0), conf.getInt("FTOption", 0), conf.getInt("tthread"), conf.getString("rootFilePath"), conf.getInt("snapshotInterval"));
            System.exit(0);
        }
        sinkThread.join((long) (30 * 1E3 * 60));//sync_ratio for sink thread to stop. Maximally sync_ratio for 10 mins
        long time_elapsed = (long) ((System.currentTimeMillis() - start) / 1E3 / 60);//in mins
        if (time_elapsed > 20) {
            if (enable_log) log.info("Program error, exist...");
            System.exit(-1);
        }

        submitter.getOM().join();
        submitter.getOM().getEM().exist();
        if (sinkThread.running) {
            if (enable_log) log.info("The application fails to stop normally, exist...");
            return -1;
        } else {
            if (enable_app_combo) {
                return SINK_CONTROL.getInstance().throughput;
            } else {
                TopologyComponent sink = submitter.getOM().g.getSink().operator;
                double sum = 0;
                int cnt = 0;
                for (ExecutionNode e : sink.getExecutorList()) {
                    double results = e.op.getResults();
                    if (results != 0) {
                        sum += results;
                    } else {
                        sum += sum / cnt;
                    }
                    cnt++;
                }
                return sum;
            }
        }
    }

    // Prepared default configuration
    private void LoadConfiguration() {
        if (configStr == null) {
            initializeCfg(config);
            switch (config.getInt("machine")) {
                case 0:
                    this.platform = new HUAWEI_Machine();
                    break;

                case 1:
                    this.platform = new HP_Machine();
                    break;
                default:
                    this.platform = new HUAWEI_Machine();
            }

            if (enable_shared_state) {
                //configure database.
                switch (config.getInt("CCOption", 0)) {
                    case Constants.CCOption_LOCK://lock_ratio
                    case Constants.CCOption_OrderLOCK://Ordered lock_ratio
                        content_type = LOCK_CONTENT;
                        break;
                    case Constants.CCOption_LWM://LWM
                        content_type = LWM_CONTENT;
                        break;
                    case Constants.CCOption_MorphStream:
                        if (config.getInt("FTOption") == 4) {
                            content_type = LVTSTREAM_CONTENT;//records the multi-version of table record.
                        } else {
                            content_type = T_STREAMCONTENT;
                        }
                        break;
                    case Constants.CCOption_SStore://SStore
                        content_type = SSTORE_CONTENT;//records the multi-version of table record.
                        break;
                    default:
                        System.exit(-1);
                }
                int tthread = config.getInt("tthread");
                if (enable_app_combo) {
                    config.put(BaseConstants.BaseConf.SPOUT_THREADS, tthread);
                }
            }

            //set overhead_total parallelism, equally parallelism
            switch (application) {
                case "GrepSum": {
                    config.put("app", 0);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(GrepSumConstants.Conf.Executor_Threads, threads);
                    break;
                }
                case "WindowedGrepSum": {
                    config.put("app", 4);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(GrepSumConstants.Conf.Executor_Threads, threads);
                    break;
                }
                case "StreamLedger": {
                    config.put("app", 1);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(SL_THREADS, threads);
                    break;
                }
                case "OnlineBiding": {
                    config.put("app", 3);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(OB_THREADS, threads);
                    break;
                }
                case "TollProcessing": {
                    config.put("app", 2);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(Executor_Threads, threads);
                    break;
                }
                case "SHJ": {
                    config.put("app", 5);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(SHJConstants.Conf.Executor_Threads, threads);
                    break;
                }
                case "NonGrepSum": {
                    config.put("app", 6);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(NonGrepSumConstants.Conf.Executor_Threads, threads);
                    break;
                }
            }
            switch (config.getInt("FTOption", 0)) {
                case 0:
                case 1:
                    loggingRecord_type = LOGOption_no;
                    break;
                case 2:
                    loggingRecord_type = LOGOption_wal;
                    break;
                case 3:
                    loggingRecord_type = LOGOption_path;
                    break;
                case 4:
                    loggingRecord_type = LOGOption_lv;
                    break;
                case 5:
                    loggingRecord_type = LOGOption_dependency;
                    break;
                case 6:
                    loggingRecord_type = LOGOption_command;
                    break;
                default:
                    System.exit(-1);
            }

            OperationChainCommon.cleanUp = config.getBoolean("cleanUp");
            FaultToleranceRelax.isHistoryView = config.getBoolean("isHistoryView");
            FaultToleranceRelax.isAbortPushDown = config.getBoolean("isAbortPushDown");
            FaultToleranceRelax.isTaskPlacing = config.getBoolean("isTaskPlacing");
            FaultToleranceRelax.isSelectiveLogging = config.getBoolean("isSelectiveLogging");
        } else {
            config.putAll(Configuration.fromStr(configStr));
        }
    }

    public void run() throws InterruptedException {
        MeasureTools.Initialize();
        LoadConfiguration();

        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }
        // In case topology names is given, create one
        if (topologyName == null) {
            topologyName = application;
        }
        // Get the topology
        Topology topology = app.getTopology(topologyName, config);
        topology.addMachine(platform);
        // Run the topology
        double rt = runTopologyLocally(topology, config);
        if (enable_profile) {
            if (rt != -1) {//returns normally.
                log.info("finished measurement (k events/s):\t" + rt);
            }
            if (enable_shared_state) {
                SpinLock[] spinlock = final_topology.spinlock;
                for (SpinLock lock : spinlock) {
                    if (lock != null)
                        log.info("Partition" + lock + " being locked:\t" + lock.count + "\t times");
                }
                METRICS_REPORT(config.getInt("CCOption", 0), config.getInt("FTOption", 0), tthread, rt, config.getInt("phaseNum"), config.getInt("shiftRate"), config.getInt("snapshotInterval"));
            }
        }//end of profile.
    }
}
