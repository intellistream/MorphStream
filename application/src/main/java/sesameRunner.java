import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import common.CONTROL;
import common.abstractRunner;
import common.collections.Configuration;
import common.collections.Constants;
import common.collections.OsUtils;
import common.constants.BaseConstants;
import common.constants.GrepSumConstants;
import common.platform.HP_Machine;
import common.platform.HUAWEI_Machine;
import common.platform.Platform;
import common.topology.transactional.GrepSum;
import common.topology.transactional.OnlineBiding;
import common.topology.transactional.StreamLedger;
import common.topology.transactional.TollProcessing;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.components.TopologyComponent;
import sesame.components.exception.UnhandledCaseException;
import sesame.execution.ExecutionNode;
import sesame.execution.runtime.executorThread;
import sesame.topology.TopologySubmitter;
import state_engine.common.SpinLock;
import state_engine.profiler.Metrics;
import state_engine.utils.SINK_CONTROL;

import java.io.*;
import java.util.Properties;

import static common.CONTROL.enable_app_combo;
import static common.CONTROL.enable_profile;
import static common.Constants.System_Plan_Path;
import static common.constants.LinearRoadConstants.Conf.Executor_Threads;
import static common.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
import static common.constants.StreamLedgerConstants.Conf.SL_THREADS;
import static state_engine.content.Content.*;
import static state_engine.content.LWMContentImpl.LWM_CONTENT;
import static state_engine.content.LockContentImpl.LOCK_CONTENT;
import static state_engine.content.SStoreContentImpl.SSTORE_CONTENT;
import static state_engine.content.T_StreamContentImpl.T_STREAMCONTENT;
import static state_engine.content.ToContentImpl.TO_CONTENT;
import static state_engine.content.common.ContentCommon.content_type;

public class sesameRunner extends abstractRunner {

    private static final Logger LOG = LoggerFactory.getLogger(sesameRunner.class);
    private static Topology final_topology;
    private final AppDriver driver;
    private final Configuration config = new Configuration();
    private Platform platform;

    private sesameRunner() {
        driver = new AppDriver();
        //Transactional Application
        driver.addApp("GrepSum", GrepSum.class);//GS
        driver.addApp("StreamLedger", StreamLedger.class);//SL
        driver.addApp("OnlineBiding", OnlineBiding.class);//OB
        driver.addApp("TollProcessing", TollProcessing.class);//TP
    }

    public static void main(String[] args) {
        sesameRunner runner = new sesameRunner();
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
        if (conf.getBoolean("simulation")) {
            System.exit(0);
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
    private void run() throws InterruptedException {
        // Loads the configuration file set by the user or the default
        // configuration
        // Prepared default configuration
        if (configStr == null) {
            String cfg = String.format(CFG_PATH, application);
            Properties p = null;
            try {
                p = loadProperties(cfg);
            } catch (IOException e) {
                e.printStackTrace();
            }
            config.putAll(Configuration.fromProperties(p));
            if (mode.equalsIgnoreCase(RUN_REMOTE)) {
                final String spout_class = String.valueOf(config.get("mb.spout.class"));
                if (spout_class.equals("applications.spout.LocalStateSpout")) {
                    LOG.info("Please use kafkaSpout in cluster mode!!!");
                    System.exit(-1);
                }
            }
            config.put(Configuration.TOPOLOGY_WORKER_CHILDOPTS, CHILDOPTS);
            configuration(config);
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
            if (simulation) {
                LOG.info("Simulation: use machine:" + config.getInt("machine")
                        + " with sockets:" + config.getInt("num_socket")
                        + " and cores:" + config.getInt("num_cpu"));
            }
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
            } else
                config.put(BaseConstants.BaseConf.SPOUT_THREADS, sthread);
            config.put(BaseConstants.BaseConf.SINK_THREADS, sithread);
            config.put(BaseConstants.BaseConf.PARSER_THREADS, pthread);
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
            Constants.default_sourceRate = config.getInt("targetHz");
        } else {
            config.putAll(Configuration.fromStr(configStr));
        }
        DescriptiveStatistics record = new DescriptiveStatistics();
        System.gc();
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
        if (CONTROL.enable_shared_state) {
            SpinLock[] spinlock = final_topology.spinlock;
            for (SpinLock lock : spinlock) {
                if (lock != null)
                    LOG.info("Partition" + lock + " being locked:\t" + lock.count + "\t times");
            }
        }
        Metrics metrics = Metrics.getInstance();
        if (rt != -1) {//returns normally.
            record.addValue(rt);
        }
        LOG.info("finished measurement (k events/s):\t" + record.getPercentile(50));
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

            System.out.println("******* STATS BEGIN *******");

            System.out.println(String.format("Time spent in pre transaction                                             : %.3f%%", (pre_txn_time/total)*100.0f));
            System.out.println(String.format("Time spent in transaction processing                                      : %.3f%%", (txn_total/total)*100.0f));
            System.out.println(String.format("Other time (read input, dump results to a file)                           : %.3f%%", ((total-pre_txn_time-txn_total)/total)*100.0f));

            System.out.println("******* PRE_TXN BREAKDOWN *******");
            System.out.println(String.format("Time spent creating Operation Chains                                      : %.3f%%", (create_oc_time/total)*100.0f));
            System.out.println(String.format("Time spent recording data dependencies                                    : %.3f%%", (dependency_checking_time/total)*100.0f));
//            System.out.println(String.format("Time spent of recording data dependencies for out of transaction checking : %.3f%%", (dependency_outoforder_overhead_time/total)*100.0f));
            System.out.println(String.format("Time spent to access DB                                                   : %.3f%%", (db_access_time/total)*100.0f));
            System.out.println(String.format("Not accounting for                                                        : %.3f%%", ((pre_txn_time-create_oc_time-dependency_checking_time-db_access_time)/total)*100.0f));

            System.out.println("******* TRANSACTION PROCESSING BREAKDOWN *******");
            System.out.println(String.format("Time spent processing transactions                                        : %.3f%%", (txn_processing/total)*100.0f));
            System.out.println(String.format("Time spent on state accessing                                             : %.3f%%", (state_access/total)*100.0f));
            System.out.println(String.format("Time spent calculating levels                                             : %.3f%%", (calculate_levels/total)*100.0f));
            System.out.println(String.format("Time spent on iterative processing                                        : %.3f%%", (iterative_processing_useful/total)*100.0f));
            System.out.println(String.format("Threads wait time and other overhead                                      : %.3f%%", ((txn_processing-calculate_levels-iterative_processing_useful)/total)*100.0f));
            System.out.println(" ");
            System.out.println(" ");
            System.out.println("**************************************");
            System.out.println("******* STATS BEGIN IN SECONDS *******");

            System.out.println(String.format("Total time                                                                : %.3f seconds", (total)/1000.0f));
            System.out.println(String.format("Time spent in pre transaction                                             : %.3f seconds", (pre_txn_time/1000.0f)));
            System.out.println(String.format("Time spent in transaction processing                                      : %.3f seconds", (txn_total/1000.0f)));
            System.out.println(String.format("Other time (read input, dump results to a file)                           : %.3f seconds", ((total-pre_txn_time-txn_total)/1000.0f)));

            System.out.println("******* PRE_TXN BREAKDOWN *******");
            System.out.println(String.format("Time spent creating Operation Chains                                      : %.3f seconds", (create_oc_time/1000.0f)));
            System.out.println(String.format("Time spent recording data dependencies                                    : %.3f seconds", (dependency_checking_time/1000.0f)));
            System.out.println(String.format("Time spent of recording data dependencies for out of transaction checking : %.3f seconds", (dependency_outoforder_overhead_time/1000.0f)));

            System.out.println("******* TRANSACTION PROCESSING BREAKDOWN *******");
            System.out.println(String.format("Time spent processing transactions                                        : %.3f seconds", (txn_processing/1000.0f)));
            System.out.println(String.format("Time spent on state accessing                                             : %.3f seconds", (state_access/1000.0f)));
            System.out.println(String.format("Time spent calculating levels                                             : %.3f seconds", (calculate_levels/1000.0f)));
            System.out.println(String.format("Time spent on iterative processing                                        : %.3f seconds", (iterative_processing_useful/1000.0f)));
            System.out.println(String.format("Threads wait time and other overhead                                      : %.3f seconds", ((txn_processing-calculate_levels-iterative_processing_useful)/1000.0f)));

            System.out.println("******* STATS ENDS *******");
            //used in TSTREAM.
            String directory = System_Plan_Path
                    + OsUtils.OS_wrapper("sesame")
                    + OsUtils.OS_wrapper(topology.getPrefix())
                    + OsUtils.OS_wrapper("CCOption=" + String.valueOf(config.getInt("CCOption", 0)));
            File file = new File(directory);
            if (!file.mkdirs()) {
            }
            FileWriter f = null;
            StringBuilder sb = new StringBuilder();
            try {
                f = new FileWriter(new File(directory + OsUtils.OS_wrapper("breakdown(" + String.valueOf(checkpoint)) + ").txt"), true);
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
                            + OsUtils.OS_wrapper("details(" + String.valueOf(tthread) + "," + String.valueOf(checkpoint)) + ").txt"), true);
                    w = new BufferedWriter(f);
                    for (int i = 0; i < tthread; i++) {
                        sb.append(String.valueOf(i));//which thread.
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
            LOG.info("===OVERALL===");
            LOG.info("Overhead on one input_event:" + String.format("%.2f", overhead));
            LOG.info("Stream Processing on one input_event:" + String.format("%.2f", stream_processing));
            LOG.info("TXN Processing on one input_event:" + String.format("%.2f", txn_processing));
            LOG.warn("Useful ratio of TStream may be very inaccurate. It is currently an estimation. Fix it later.");
            LOG.info("===BREAKDOWN TXN===");
            LOG.info("Useful ratio:\t" + String.format("%.2f", useful_ratio));
            LOG.info("Index ratio:\t" + String.format("%.2f", index_ratio));
            LOG.info("Wait ratio:\t" + String.format("%.2f", wait_ratio));
            LOG.info("lock ratio:\t" + String.format("%.2f", lock_ratio));
            LOG.info("====Details ====");
            LOG.info("\n" + sb.toString());
        }
    }
}
