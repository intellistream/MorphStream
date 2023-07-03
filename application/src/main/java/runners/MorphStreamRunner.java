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
import static content.LWMContentImpl.LWM_CONTENT;
import static content.LockContentImpl.LOCK_CONTENT;
import static content.SStoreContentImpl.SSTORE_CONTENT;
import static content.TStreamContentImpl.T_STREAMCONTENT;
import static content.common.ContentCommon.content_type;
import static profiler.MeasureTools.METRICS_REPORT;
import static profiler.Metrics.timer;

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
                        content_type = T_STREAMCONTENT;//records the multi-version of table record.
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
            OperationChainCommon.cleanUp = config.getBoolean("cleanUp");
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
            timer.scheduleAtFixedRate(new Metrics.RuntimeMemory(),0,  500);
        }
        TopologySubmitter submitter = new TopologySubmitter();
        try {
            final_topology = submitter.submitTopology(topology, conf);
        } catch (UnhandledCaseException e) {
            e.printStackTrace();
        }
        executorThread sinkThread = submitter.getOM().getEM().getSinkThread();
        long start = System.currentTimeMillis();
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

                // decide the output path of metrics.
                String statsFolderPattern = OsUtils.osWrapperPostFix(rootPath)
                        + OsUtils.osWrapperPostFix("stats")
                        + OsUtils.osWrapperPostFix("%s")
                        + OsUtils.osWrapperPostFix("%s")
                        + OsUtils.osWrapperPostFix("threads = %d")
                        + OsUtils.osWrapperPostFix("totalEvents = %d")
                        + OsUtils.osWrapperPostFix("%d_%d_%d_%d_%d_%d_%s_%d");

                if (config.getInt("CCOption") == CCOption_SStore) {
                    scheduler = "PAT";
                }

                String statsFolderPath;
                if (config.getString("common").equals("StreamLedger")) {
                    statsFolderPath = String.format(statsFolderPattern,
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
                    statsFolderPath = String.format(statsFolderPattern,
                            config.getString("common"), scheduler, tthread, totalEvents,
                            config.getInt("NUM_ITEMS"),
                            config.getInt("Ratio_of_Multiple_State_Access"),
                            config.getInt("State_Access_Skewness"),
                            config.getInt("Ratio_of_Overlapped_Keys"),
                            config.getInt("Ratio_of_Transaction_Aborts"),
                            config.getInt("Transaction_Length"),
                            AppConfig.isCyclic,
                            config.getInt("complexity"));
                } else if (config.getString("common").equals("OnlineBiding")){
                    statsFolderPath = String.format(statsFolderPattern,
                            config.getString("common"), scheduler, tthread, totalEvents,
                            config.getInt("NUM_ITEMS"),
                            config.getInt("NUM_ACCESS"),
                            config.getInt("State_Access_Skewness"),
                            config.getInt("Ratio_of_Overlapped_Keys"),
                            config.getInt("Ratio_of_Transaction_Aborts"),
                            config.getInt("Transaction_Length"),
                            AppConfig.isCyclic,
                            config.getInt("complexity"));
                } else if (config.getString("common").equals("TollProcessing")){
                    statsFolderPath = String.format(statsFolderPattern,
                            config.getString("common"), scheduler, tthread, totalEvents,
                            config.getInt("NUM_ITEMS"),
                            config.getInt("NUM_ACCESS"),
                            config.getInt("State_Access_Skewness"),
                            config.getInt("Ratio_of_Overlapped_Keys"),
                            config.getInt("Ratio_of_Transaction_Aborts"),
                            config.getInt("Transaction_Length"),
                            AppConfig.isCyclic,
                            config.getInt("complexity"));
                } else if (config.getString("common").equals("WindowedGrepSum")) {
                    statsFolderPath = String.format(statsFolderPattern,
                            config.getString("common"), scheduler, tthread, totalEvents,
                            config.getInt("NUM_ITEMS"),
                            config.getInt("Ratio_of_Multiple_State_Access"),
                            config.getInt("State_Access_Skewness"),
                            config.getInt("Period_of_Window_Reads"),
                            config.getInt("windowSize"),
                            config.getInt("Transaction_Length"),
                            AppConfig.isCyclic,
                            config.getInt("complexity"));
                } else if (config.getString("common").equals("SHJ")) {
                    statsFolderPath = String.format(statsFolderPattern,
                            config.getString("common"), scheduler, tthread, totalEvents,
                            config.getInt("NUM_ITEMS"),
                            config.getInt("Ratio_of_Multiple_State_Access"),
                            config.getInt("State_Access_Skewness"),
                            config.getInt("Ratio_of_Overlapped_Keys"),
                            config.getInt("Ratio_of_Transaction_Aborts"),
                            config.getInt("Transaction_Length"),
                            AppConfig.isCyclic,
                            config.getInt("complexity"));
                } else if (config.getString("common").equals("NonGrepSum")) {
                    statsFolderPath = String.format(statsFolderPattern,
                            config.getString("common"), scheduler, tthread, totalEvents,
                            config.getInt("NUM_ITEMS"),
                            config.getInt("NUM_ACCESS"),
                            config.getInt("State_Access_Skewness"),
                            config.getInt("Ratio_of_Non_Deterministic_State_Access"),
                            config.getInt("Ratio_of_Transaction_Aborts"),
                            config.getInt("Transaction_Length"),
                            AppConfig.isCyclic,
                            config.getInt("complexity"));
                } else
                    throw new UnsupportedOperationException();
                File file = new File(statsFolderPath);
                log.info("Dumping stats to...");
                log.info(String.valueOf(file.getAbsoluteFile()));
                file.mkdirs();

                if (file.exists())
                    file.delete();
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                METRICS_REPORT(config.getInt("CCOption", 0), file, tthread, rt, config.getInt("phaseNum"), config.getInt("shiftRate"));
            }
        }//end of profile.
    }
}
