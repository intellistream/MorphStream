package runners;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import common.Runner;
import common.SpinLock;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.constants.BaseConstants;
import common.constants.GrepSumConstants;
import common.platform.HP_Machine;
import common.platform.HUAWEI_Machine;
import common.platform.Platform;
import common.topology.WordCount;
import common.topology.transactional.GrepSum;
import common.topology.transactional.OnlineBiding;
import common.topology.transactional.StreamLedger;
import common.topology.transactional.TollProcessing;
import components.Topology;
import components.TopologyComponent;
import components.exception.UnhandledCaseException;
import execution.ExecutionNode;
import execution.runtime.executorThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.Metrics;
import topology.TopologySubmitter;
import utils.SINK_CONTROL;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import static common.CONTROL.*;
import static common.constants.LinearRoadConstants.Conf.Executor_Threads;
import static common.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
import static common.constants.StreamLedgerConstants.Conf.SL_THREADS;
import static content.Content.CCOption_OTS;
import static content.LWMContentImpl.LWM_CONTENT;
import static content.LockContentImpl.LOCK_CONTENT;
import static content.SStoreContentImpl.SSTORE_CONTENT;
import static content.T_StreamContentImpl.T_STREAMCONTENT;
import static content.ToContentImpl.TO_CONTENT;
import static content.common.ContentCommon.content_type;

public class TStreamRunner extends Runner {

    private static final Logger LOG = LoggerFactory.getLogger(TStreamRunner.class);
    private static Topology final_topology;
    private final AppDriver driver;
    private final Configuration config = new Configuration();
    private Platform platform;

    public TStreamRunner() {

        driver = new AppDriver();

        //Ordinary Application
        driver.addApp("WordCount", WordCount.class);//WC

        //Transactional Application
        driver.addApp("GrepSum", GrepSum.class);//GS
        driver.addApp("StreamLedger", StreamLedger.class);//SL
        driver.addApp("OnlineBiding", OnlineBiding.class);//OB
        driver.addApp("TollProcessing", TollProcessing.class);//TP
    }

    public static void main(String[] args) {
        LOG.info("Program Starts..");
        TStreamRunner runner = new TStreamRunner();
        JCommander cmd = new JCommander(runner);
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
        }
        try {
            runner.run();
        } catch (InterruptedException ex) {
            LOG.error("Error in running topology locally", ex);
        }
    }

    private static double runTopologyLocally(Topology topology, Configuration conf) throws InterruptedException {
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
            LOG.info("Program error, exist...");
            System.exit(-1);
        }

        submitter.getOM().join();
        submitter.getOM().getEM().exist();
        if (sinkThread.running) {
            LOG.info("The application fails to stop normally, exist...");
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

        if (rt != -1) {//returns normally.
            LOG.info("finished measurement (k events/s):\t" + rt);
        }

        if (enable_shared_state) {
            SpinLock[] spinlock = final_topology.spinlock;
            for (SpinLock lock : spinlock) {
                if (lock != null)
                    LOG.info("Partition" + lock + " being locked:\t" + lock.count + "\t times");
            }

            Metrics metrics = Metrics.getInstance();
            if (enable_profile) {
                double overhead = 0;
                double stream_processing = 0;
                double useful_ratio = 0;
                double abort_time = 0;
                double ts_alloc_time = 0;
                double index_ratio = 0;
                double wait_ratio = 0;
                double lock_ratio = 0;
                double compute_time = 0;
                double total = 0;
                double txn_total = 0;
                double txn_processing = 0;
                double state_access = 0;
                double calculate_levels = 0;
                double iterative_processing_useful = 0;

                double pre_txn_time = 0;
                double create_oc_time = 0;
                double dependency_checking_time = 0;
                double dependency_outoforder_overhead_time = 0;
                double db_access_time = 0;

                String statsFolderPattern = OsUtils.osWrapperPostFix(rootPath)
                        + OsUtils.osWrapperPostFix("stats")
                        + OsUtils.osWrapperPostFix("scheduler = %s")
                        + OsUtils.osWrapperPostFix("depth = %d")
                        + OsUtils.osWrapperPostFix("threads = %d")
                        + OsUtils.osWrapperPostFix("total_batches = %d")
                        + OsUtils.osWrapperPostFix("events_per_batch = %d");

                String statsFolderPath = String.format(statsFolderPattern, scheduler, numberOfDLevels, tthread, numberOfBatches, totalEventsPerBatch);

                try {

                    File file = new File(statsFolderPath + String.format("iteration_%d.csv", iterationNumber));
                    LOG.info("Dumping stats to...");
                    LOG.info(String.valueOf(file.getAbsoluteFile()));
                    file.mkdirs();

                    if (file.exists())
                        file.delete();
                    file.createNewFile();

                    BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(file.getPath()));
                    fileWriter.write("thread_id, batch_id, total_tm, pre_process_tm, oc_creation_tm, d_recording_tm, delayed_d_recording_tm, txn_tm, iterative_processing_tm, useful_iterative_processing_tm, level_calc_tm, barrier_tm, no_of_transactions, no_of_ocs, submit_ttl_tm, submit_barrier_tm, submit_overhead_tm, submit_prm_1_tm, submit_prm_2_tm, submit_prm_3_tm, get_next_ttl_tm, get_next_overhead_tm, get_next_barrier_tm, get_next_thread_wait_tm, get_next_prm_1_tm, get_next_prm_2_tm, get_next_prm_3_tm,\n");

                    for (int threadId = 0; threadId < tthread; threadId++) {
                        long batches = metrics.total[threadId].getN();
                        for (int batch = 0; batch < batches; batch++) {
                            fileWriter.write(String.format("%d, %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f,\n"
                                    , threadId
                                    , batch
                                    , metrics.total[threadId].getElement(batch)
                                    , metrics.pre_txn_total[threadId].getElement(batch)
                                    , metrics.create_oc_total[threadId].getElement(batch)
                                    , metrics.dependency_checking_total[threadId].getElement(batch)
                                    , metrics.dependency_outoforder_overhead_total[threadId].getElement(batch)
                                    , metrics.txn_total[threadId].getElement(batch) // total time for transaction processing step,
                                    , metrics.txn_processing_total[threadId].getElement(batch)  // time for transaction processing.
                                    , metrics.iterative_processing_useful_total[threadId].getElement(batch)  // time for transaction processing.
                                    , metrics.calculate_levels_total[threadId].getElement(batch)
                                    , metrics.barriers_total[threadId].getElement(batch)
                                    , metrics.numberOf_transactional_events_processed[threadId].getElement(batch)
                                    , metrics.number_of_ocs_processed[threadId].getElement(batch)

                                    , metrics.submit_time_total[threadId].getElement(batch)
                                    , metrics.submit_time_barrier_total[threadId].getElement(batch)
                                    , metrics.submit_time_overhead_total[threadId].getElement(batch)
                                    , metrics.submit_time_extra_param_1_total[threadId].getElement(batch)
                                    , metrics.submit_time_extra_param_2_total[threadId].getElement(batch)
                                    , metrics.submit_time_extra_param_3_total[threadId].getElement(batch)

                                    , metrics.get_next_time_total[threadId].getElement(batch)
                                    , metrics.get_next_overhead_time_total[threadId].getElement(batch)
                                    , metrics.get_next_barrier_time_total[threadId].getElement(batch)
                                    , metrics.get_next_thread_wait_time_total[threadId].getElement(batch)
                                    , metrics.get_next_extra_param_1_total[threadId].getElement(batch)
                                    , metrics.get_next_extra_param_2_total[threadId].getElement(batch)
                                    , metrics.get_next_extra_param_3_total[threadId].getElement(batch)));
                        }
                    }
                    fileWriter.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                for (int i = 0; i < tthread; i++) {

                    useful_ratio += metrics.useful_ratio[i].getMean();
                    index_ratio += metrics.index_ratio[i].getMean();
                    wait_ratio += metrics.sync_ratio[i].getMean();
                    if (config.getInt("CCOption", 0) != CCOption_TStream)
                        lock_ratio += metrics.lock_ratio[i].getMean();
                    stream_processing += metrics.stream_total[i].getMean();
                    overhead += metrics.overhead_total[i].getMean();

                    total += metrics.total[i].getMean();

                    txn_total += metrics.txn_total[i].getMean();
                    txn_processing += metrics.txn_processing_total[i].getMean();
                    state_access += metrics.state_access_total[i].getMean();
                    calculate_levels += metrics.calculate_levels_total[i].getMean();
                    iterative_processing_useful += metrics.iterative_processing_useful_total[i].getMean();

                    pre_txn_time += metrics.pre_txn_total[i].getMean();
                    create_oc_time += metrics.create_oc_total[i].getMean();
                    dependency_checking_time += metrics.dependency_checking_total[i].getMean();
                    dependency_outoforder_overhead_time += metrics.dependency_outoforder_overhead_total[i].getMean();
                    db_access_time += metrics.db_access_time[i].getMean();

                }
                //get average ratio per thread.
                useful_ratio = useful_ratio / tthread;
                abort_time = abort_time / tthread;
                ts_alloc_time = ts_alloc_time / tthread;
                index_ratio = index_ratio / tthread;
                wait_ratio = wait_ratio / tthread;
                lock_ratio = lock_ratio / tthread;
                compute_time = compute_time / tthread;
                stream_processing = stream_processing / tthread;
                overhead = overhead / tthread;

                total = total / tthread;

                txn_total = txn_total / tthread;
                txn_processing = txn_processing / tthread;
                state_access = state_access / tthread;
                calculate_levels = calculate_levels / tthread;
                iterative_processing_useful = iterative_processing_useful / tthread;

                pre_txn_time = pre_txn_time / tthread;
                create_oc_time = create_oc_time / tthread;
                dependency_checking_time = dependency_checking_time / tthread;
                dependency_outoforder_overhead_time = dependency_outoforder_overhead_time / tthread;
                db_access_time = db_access_time / tthread;

//            LOG.info("******* STATS BEGIN *******");
//
//            LOG.info(String.format("Time spent in pre transaction                                             : %.3f%%", (pre_txn_time/total)*100.0f));
//            LOG.info(String.format("Time spent in transaction processing                                      : %.3f%%", (txn_total/total)*100.0f));
//            LOG.info(String.format("Other time (read input, dump results to a file)                           : %.3f%%", ((total-pre_txn_time-txn_total)/total)*100.0f));
//
//            LOG.info("******* PRE_TXN BREAKDOWN *******");
//            LOG.info(String.format("Time spent creating Operation Chains                                      : %.3f%%", (create_oc_time/total)*100.0f));
//            LOG.info(String.format("Time spent recording data dependencies                                    : %.3f%%", (dependency_checking_time/total)*100.0f));
//            LOG.info(String.format("Time spent to access DB                                                   : %.3f%%", (db_access_time/total)*100.0f));
//            LOG.info(String.format("Not accounting for                                                        : %.3f%%", ((pre_txn_time-create_oc_time-dependency_checking_time-db_access_time)/total)*100.0f));
//
//            LOG.info("******* TRANSACTION PROCESSING BREAKDOWN *******");
//            LOG.info(String.format("Time spent processing transactions                                        : %.3f%%", (txn_processing/total)*100.0f));
//            LOG.info(String.format("Time spent on state accessing                                             : %.3f%%", (state_access/total)*100.0f));
//            LOG.info(String.format("Time spent calculating levels                                             : %.3f%%", (calculate_levels/total)*100.0f));
//            LOG.info(String.format("Time spent on iterative processing                                        : %.3f%%", (iterative_processing_useful/total)*100.0f));
//            LOG.info(String.format("Threads wait time and other overhead                                      : %.3f%%", ((txn_processing-calculate_levels-iterative_processing_useful)/total)*100.0f));
//            LOG.info("**************************************");
                LOG.info("******* STATS BEGIN IN SECONDS *******");

                LOG.info(String.format("Total time  (per tuple)                                                              : %.3f seconds", (total) / 1000000000.0f));
                LOG.info(String.format("Time spent in pre transaction           (per tuple)                                     : %.3f seconds", (pre_txn_time / 1000000000.0f)));
                LOG.info(String.format("Time spent in transaction processing      (per tuple)                                 : %.3f seconds", (txn_total / 1000000000.0f)));
                LOG.info(String.format("Other time (read input, dump results to a file)        (per tuple)                      : %.3f seconds", ((total - pre_txn_time - txn_total) / 1000000000.0f)));

                LOG.info("******* PRE_TXN BREAKDOWN *******");
                LOG.info(String.format("Time spent creating Operation Chains                                      : %.3f seconds", (create_oc_time / 1000000000.0f)));
                LOG.info(String.format("Time spent recording data dependencies                                    : %.3f seconds", (dependency_checking_time / 1000000000.0f)));
                LOG.info(String.format("Time spent of recording data dependencies for out of transaction checking : %.3f seconds", (dependency_outoforder_overhead_time / 1000000000.0f)));

                LOG.info("******* TRANSACTION PROCESSING BREAKDOWN *******");
                LOG.info(String.format("Time spent processing transactions                                        : %.3f seconds", (txn_processing / 1000000000.0f)));
                LOG.info(String.format("Time spent on state accessing                                             : %.3f seconds", (state_access / 1000000000.0f)));
                LOG.info(String.format("Time spent calculating levels                                             : %.3f seconds", (calculate_levels / 1000000000.0f)));
                LOG.info(String.format("Time spent on iterative processing                                        : %.3f seconds", (iterative_processing_useful / 1000000000.0f)));
                LOG.info(String.format("Threads wait time and other overhead                                      : %.3f seconds", ((txn_processing - calculate_levels - iterative_processing_useful) / 1000000000.0f)));

                LOG.info("******* STATS ENDS *******");
                //used in TSTREAM.
                String directory = metric_path
                        + OsUtils.OS_wrapper("TStreamPlus")
                        + OsUtils.OS_wrapper(topology.getPrefix())
                        + OsUtils.OS_wrapper("CCOption=" + config.getInt("CCOption", 0));
                File file = new File(directory);
                if (!file.mkdirs()) {
                }
                FileWriter f = null;
                StringBuilder sb = new StringBuilder();
                try {
                    f = new FileWriter(new File(directory + OsUtils.OS_wrapper("breakdown(" + checkpoint_interval) + ").txt"), true);
                    Writer w = new BufferedWriter(f);
                    w.write(String.valueOf(tthread));
                    w.write(",");
                    w.write(String.format("%.2f", overhead));//overhead per event
                    w.write(",");
                    w.write(String.format("%.2f", stream_processing));//average stream processing.
                    w.write(",");
                    w.write(String.format("%.2f", txn_processing * (useful_ratio)));//average txn processing * useful = state access.
                    w.write(",");
                    w.write(String.format("%.2f", txn_processing * (1 - (useful_ratio))));//state access overhead.
                    w.write(",");
                    w.write(String.format("%.2f", txn_processing));//average txn processing time.
                    w.write(",");
                    w.write(String.format("%.2f", useful_ratio));//useful ratio.
                    w.write(",");
                    w.write(String.format("%.2f", abort_time));//abort ratio.
                    w.write(",");
                    w.write(String.format("%.2f", wait_ratio));//sync ratio.
                    w.write(",");
                    w.write(String.format("%.2f", lock_ratio));//lock ratio.
                    w.write(",");
                    w.write(String.format("%.2f", 1 - (useful_ratio + abort_time + wait_ratio + lock_ratio)));//others ratio.
                    w.write(",");
                    w.write(String.format("%.2f", rt));//throughput
                    w.write("\n");
                    w.close();
                    f.close();
                    if (config.getInt("CCOption", 0) == CCOption_TStream) {//extra info
                        f = new FileWriter(new File(directory
                                + OsUtils.OS_wrapper("details(" + tthread + "," + checkpoint_interval) + ").txt"), true);
                        w = new BufferedWriter(f);
                        for (int i = 0; i < tthread; i++) {
                            sb.append(i);//which thread.
                            sb.append(",");
                            sb.append(String.format("%d", metrics.useful_ratio[i].getN()));//number of txns processed by the thread.
                            sb.append(",");
                            sb.append(String.format("%.2f", metrics.average_txn_construct[i].getPercentile(50)));//average construction time.
                            sb.append(",");
                            sb.append(String.format("%.2f", metrics.average_tp_submit[i].getPercentile(50)));//average submit time.
                            sb.append(",");
                            sb.append(String.format("%.2f", metrics.average_tp_w_syn[i].getPercentile(50) - metrics.average_tp_core[i].getPercentile(50)));//average sync time.
                            sb.append(",");
                            sb.append(String.format("%.2f", metrics.average_tp_core[i].getPercentile(50)));//average core tp time.
                            sb.append("\n");
                        }
                        w.write(sb.toString());
                        w.close();
                        f.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }//end of profile.

        }
    }

    // Loads the configuration file set by the user or the default
    // configuration
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
                    case CCOption_TStream:
                        content_type = T_STREAMCONTENT;//records the multi-version of table record.
                        break;
                    case CCOption_SStore://SStore
                        content_type = SSTORE_CONTENT;//records the multi-version of table record.
                        break;
                    case CCOption_OTS://SStore
                        content_type = TO_CONTENT;//records the multi-version of table record.
                        break;
                    default:
                        throw new UnsupportedOperationException("Please define correct content type!");
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
                case "StreamLedger": {
                    config.put("app", 1);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(SL_THREADS, threads);
                    break;
                }
                case "OnlineBiding": {
                    config.put("app", 2);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(OB_THREADS, threads);
                    break;
                }
                case "TollProcessing": {
                    config.put("app", 3);
                    int threads = Math.max(1, (int) Math.floor((tthread)));
                    config.put(Executor_Threads, threads);
                    break;
                }
            }

        } else {
            config.putAll(Configuration.fromStr(configStr));
        }
    }
}
