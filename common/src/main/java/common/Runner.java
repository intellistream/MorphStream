package common;

import com.beust.jcommander.Parameter;
import common.collections.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

public abstract class Runner implements IRunner {
    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);
    protected static String CFG_PATH = null;
    /**
     * Workload Specific Parameters.
     */
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed")
    public String application = "StreamLedger";
//    public String application = "GrepSum";
    @Parameter(names = {"-t", "--topology-name"}, required = false, description = "The name of the application")
    public String topologyName;
    @Parameter(names = {"--COMPUTE_COMPLEXITY"}, description = "COMPUTE_COMPLEXITY per event")
    public int COMPUTE_COMPLEXITY = 0;// 1, 10, 100
    @Parameter(names = {"--POST_COMPUTE"}, description = "POST COMPUTE_COMPLEXITY per event")
    public int POST_COMPUTE = 0;// 1, 10, 100
    @Parameter(names = {"--NUM_ITEMS"}, description = "NUM_ITEMS in DB.")
    public int NUM_ITEMS = 1_000_000;//
    @Parameter(names = {"--NUM_ACCESS"}, description = "Number of state access per transaction")
    public int NUM_ACCESS = 10;//
    @Parameter(names = {"--scale_factor"}, description = "scale_factor")
    public double scale_factor = 1; //<=1
    @Parameter(names = {"--ratio_of_read"}, description = "ratio_of_read")
    public double ratio_of_read = 0.0; //<=1
    @Parameter(names = {"--ratio_of_multi_partition"}, description = "ratio_of_multi_partition")
    public double ratio_of_multi_partition = 0; //<=1
    @Parameter(names = {"--number_partitions"}, description = "number_partitions")
    public int number_partitions = 3;
    @Parameter(names = {"--theta"}, description = "theta")
    public double theta = 0.6; //0.6==medium contention
    @Parameter(names = {"--size_tuple"}, description = "size_tuple (number of elements in state)")
    public int size_tuple = 0;
    /**
     * System Tuning Parameters.
     */

    @Parameter(names = {"--linked"}, description = "Communication Queue as Linked List or Array (default).")
    public boolean linked = false;
    @Parameter(names = {"--shared"}, description = "Communication Queue  is shared (default) by multi producers.")
    public boolean shared = true;
    @Parameter(names = {"-bt"}, description = "Batch Emit.", required = false)
    public int batch = 1;
    @Parameter(names = {"-queue_size"}, description = "Output queue size limit.", required = false)
    public int queue_size = 10000;
    /**
     * TStream Specific Parameters.
     */
    @Parameter(names = {"--disable_pushdown"}, description = "Push down write operations to engine, it is enabled by default.")
    public boolean disable_pushdown = false;
    @Parameter(names = {"--checkpoint_interval"}, description = "checkpoint interval (seconds)")
    public double checkpoint_interval = 5;// default checkpoint interval.
    @Parameter(names = {"--TP"}, description = "TP threads")
    public int TP = 4;// default TP threads
    @Parameter(names = {"--tthread"}, description = "total execution threads")
    public int tthread = 4;// default total execution threads
    @Parameter(names = {"--CCOption"}, description = "Selecting different concurrency control options.")
    public int CCOption = CCOption_TStream;
    @Parameter(names = {"--partition"}, description = "Partitioning database. It must be enabled for S-Store scheme and it is optional for TStream scheme.")
    public boolean enable_partition = false;
    /**
     * Benchmarking Specific Parameters.
     */
    @Parameter(names = {"--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;
    @Parameter(names = {"--measure"}, description = "enable measurement")
    public boolean enable_measurement = false;
    @Parameter(names = {"--rootFilePath"}, description = "Root path for data files.")
    public String rootPath = System.getProperty("user.home") + OsUtils.OS_wrapper("tstreamplus") + OsUtils.OS_wrapper("data");
    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = rootPath + OsUtils.OS_wrapper("metric_output");
    @Parameter(names = {"--machine"}, description = "which machine to use? 0 (default): a simple one-socket machine with four cores. Add your machine specification accordingly and select them by change this specification")
    public int machine = 0;
    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the Brisk.topology (local mode only)")
    public int runtimeInSeconds = 30;
    @Parameter(names = {"--verbose"}, description = "whether print execution detail")
    public boolean verbose = false;
    /**
     * @author: Aqif
     * Input Dependency Benchmark. Bundled with StreamingLedger Application.
     * Scheduler Types:
     * BL (Barrier-based Hashed Scheduler),
     * RR (Barrier-based Round Robin Scheduler),
     * SW (Barrier-based Shared Workload Scheduler),
     * NB_BL (Greedy Hashed Scheduler),
     * NB_RR (Greedy Round Robin Scheduler),
     * NB_SW (Greedy Sahred Workload Scheduler),
     * G_S (Greedy-smart Scheduler),
     */
    @Parameter(names = {"--totalEventsPerBatch"}, description = "Total number of events per batch.")
    public int totalEventsPerBatch = 100000;
    @Parameter(names = {"--numberOfBatches"}, description = "Total number of batches.")
    public int numberOfBatches = 1;
    @Parameter(names = {"--numberOfDLevels"}, description = "Maximum number of input data dependency levels.")
    public Integer numberOfDLevels = 1024;
    @Parameter(names = {"--iterationNumber"}, description = "Number of dependency levels.")
    public Integer iterationNumber = 0;
    @Parameter(names = {"--scheduler"}, description = "Scheduler for TStream.")
    public String scheduler = "G_S";
    @Parameter(names = {"--fanoutDist"}, description = "Fanout rate distribution scheme. [uniform, zipfinv, zipf, zipfcenter]")
    public String fanoutDist = "uniform";
    @Parameter(names = {"--idGenType"}, description = "State ids distribution scheme.[uniform, normal]")
    public String idGenType = "uniform";
    /**
     * Functional Parameters.
     */
    @Parameter(names = {"--fault_tolerance"}, description = "Enable or disable fault tolerance, it is disabled by default.")
    boolean enable_fault_tolerance = false;

    public Runner() {
        CFG_PATH = "/config/%s.properties";
        LOG.info(String.format("Metric folder path %s.", metric_path));
    }

    public Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is = Runner.class.getResourceAsStream(filename);
        properties.load(is);
        is.close();
        return properties;
    }

    public void initializeCfg(HashMap<String, Object> config) {
        config.put("enable_fault_tolerance", enable_fault_tolerance);
        config.put("queue_size", queue_size);
        config.put("disable_pushdown", disable_pushdown);
        config.put("common", application);
        config.put("ratio_of_multi_partition", ratio_of_multi_partition);
        config.put("number_partitions", number_partitions);
        config.put("machine", machine);
        config.put("totalEventsPerBatch", totalEventsPerBatch);
        config.put("numberOfBatches", numberOfBatches);
        config.put("rootFilePath", rootPath);
        config.put("scheduler", scheduler);
        config.put("fanoutDist", fanoutDist);
        config.put("idGenType", idGenType);
        config.put("numberOfDLevels", numberOfDLevels);

        if (CCOption == 4)//S-Store enabled.
            config.put("partition", true);
        else
            config.put("partition", enable_partition);
        config.put("measure", enable_measurement);
        config.put("checkpoint", checkpoint_interval);
        config.put("TP", TP);
        config.put("tthread", tthread);
        config.put("COMPUTE_COMPLEXITY", COMPUTE_COMPLEXITY);
        config.put("POST_COMPUTE", POST_COMPUTE);
        config.put("NUM_ACCESS", NUM_ACCESS);
        config.put("NUM_ITEMS", NUM_ITEMS);
        config.put("CCOption", CCOption);
        config.put("linked", linked);
        config.put("shared", shared);
        config.put("scale_factor", scale_factor);
        config.put("ratio_of_read", ratio_of_read);
        config.put("theta", theta);

        if (batch != -1) {
            config.put("batch", batch);
        }
        config.put("metrics.output", metric_path);
        config.put("runtimeInSeconds", runtimeInSeconds);
        config.put("size_tuple", size_tuple);
        config.put("verbose", verbose);
        System.setProperty("my.log", metric_path);
    }
}
