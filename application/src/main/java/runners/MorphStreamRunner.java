package runners;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import common.Runner;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.constants.BaseConstants;
import common.constants.GrepSumConstants;
import common.constants.NonGrepSumConstants;
import common.constants.SHJConstants;
import common.platform.HP_Machine;
import common.platform.HUAWEI_Machine;
import common.platform.Platform;
import common.topology.WordCount;
import common.topology.transactional.*;
import components.Topology;
import components.TopologyComponent;
import components.exception.UnhandledCaseException;
import durability.struct.FaultToleranceRelax;
import execution.ExecutionNode;
import execution.runtime.executorThread;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import profiler.Metrics;
import scheduler.struct.OperationChainCommon;
import topology.TopologySubmitter;
import utils.AppConfig;
import utils.SINK_CONTROL;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static common.CONTROL.*;
import static common.constants.LinearRoadConstants.Conf.Executor_Threads;
import static common.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
import static common.constants.StreamLedgerConstants.Conf.SL_THREADS;
import static content.LVTStreamContent.LVTSTREAM_CONTENT;
import static content.LWMContentImpl.LWM_CONTENT;
import static content.LockContentImpl.LOCK_CONTENT;
import static content.SStoreContentImpl.SSTORE_CONTENT;
import static content.TStreamContentImpl.T_STREAMCONTENT;
import static content.common.ContentCommon.content_type;
import static content.common.ContentCommon.loggingRecord_type;
import static profiler.MeasureTools.METRICS_REPORT;
import static profiler.MeasureTools.METRICS_REPORT_WITH_FAILURE;
import static profiler.Metrics.timer;
import static utils.FaultToleranceConstants.*;
import static utils.FaultToleranceConstants.LOGOption_command;

public class MorphStreamRunner extends Runner {
    private static final Logger log = LoggerFactory.getLogger(MorphStreamRunner.class);
    private static Topology final_topology;
    private final AppDriver driver;
    private final Configuration config = new Configuration();
    private Platform platform;

    public MorphStreamRunner() {
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

    // Prepared default configuration
    private void LoadConfiguration() {
        if (configStr == null) {
            String cfg = String.format(CFG_PATH, application);
            Properties p = null;
            try {
                p = loadProperties(cfg);
            } catch (IOException e) {
                e.printStackTrace();
            }
            config.putAll(Configuration.fromProperties(p));

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
                    case CCOption_LOCK://lock_ratio
                    case CCOption_OrderLOCK://Ordered lock_ratio
                        content_type = LOCK_CONTENT;
                        break;
                    case CCOption_LWM://LWM
                        content_type = LWM_CONTENT;
                        break;
                    case CCOption_MorphStream:
                        if (config.getInt("FTOption") == 4) {
                            content_type = LVTSTREAM_CONTENT;//records the multi-version of table record.
                        } else {
                            content_type = T_STREAMCONTENT;
                        }
                        break;
                    case CCOption_SStore://SStore
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

    public static void main(String[] args) {
        if (enable_log) log.info("Program Starts..");
        MorphStreamRunner runner = new MorphStreamRunner();
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
            timer.scheduleAtFixedRate(new Metrics.RuntimeHardware(conf.getString("rootFilePath")),0,  10);
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
                METRICS_REPORT(config, tthread, rt);
            }
        }//end of profile.
    }
}
