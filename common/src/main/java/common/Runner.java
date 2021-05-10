package common;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;
import common.collections.Constants;
import common.collections.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
/**
 * Created by I309939 on 7/30/2016.
 */
public abstract class Runner implements IRunner {
    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);
    protected static String CFG_PATH = null;

    /**
     * Functional Parameters.
     */
    @Parameter(names = {"--fault_tolerance"}, description = "Enable or disable fault tolerance, it is disabled by default.")
    boolean enable_fault_tolerance = false;

    @Parameter(names = {"--transaction"}, description = "Enable or disable transactional state management, it is enabled by default.")
    boolean enable_transaction = true;

    /**
     * System Tuning Parameters.
     */
    @Parameter(names = {"--CCOption"}, description = "Selecting different concurrency control options.")
    public int CCOption = CCOption_TStream;

    @Parameter(names = {"--partition"}, description = "Partitioning database. It must be enabled for S-Store scheme and it is optional for TStream scheme.")
    public boolean enable_partition = false;

    /**
     * TStream Specific Parameters.
     */
    @Parameter(names = {"--disable_pushdown"}, description = "Push down write operations to engine, it is enabled by default.")
    public boolean disable_pushdown = false;

    @Parameter(names = {"--checkpoint_interval"}, description = "checkpoint interval (seconds)")
    public double checkpoint_interval = 500;// default checkpoint interval.

    @Parameter(names = {"--linked"}, description = "Communication Queue as Linked List or Array (default).")
    public boolean linked = false;
    @Parameter(names = {"--shared"}, description = "Communication Queue  is shared (default) by multi producers.")
    public boolean shared = true;

    /**
     * Benchmarking Specific Parameters.
     */
    @Parameter(names = {"--measure"}, description = "enable measurement")
    public boolean enable_measurement = false;

    @Parameter(names = {"--rootFilePath"}, description = "Root path for data files.")
    public String rootPath = System.getProperty("user.home") + OsUtils.OS_wrapper("tstreamplus") + OsUtils.OS_wrapper("data");

    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = rootPath + OsUtils.OS_wrapper("metric_output");

    @Parameter(names = {"--machine"}, description = "which machine to use? 0:NUS machine, 1: HPI machine, you may add more..")
    public int machine = 10;

    /**
     * Workload Specific Parameters.
     */
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed", required = false)
    public String application = "StreamLedger";
    @Parameter(names = {"--COMPUTE_COMPLEXITY"}, description = "COMPUTE_COMPLEXITY per event")
    public int COMPUTE_COMPLEXITY = 0;// 1, 10, 100
    @Parameter(names = {"--POST_COMPUTE"}, description = "POST COMPUTE_COMPLEXITY per event")
    public int POST_COMPUTE = 0;// 1, 10, 100
    @Parameter(names = {"--NUM_ITEMS"}, description = "NUM_ITEMS in DB.")
    public int NUM_ITEMS = 100_000;//
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


    @Parameter(names = {"--plan"}, description = "benchmarking the corresponding plan")
    public int plan = 0;
    @Parameter(names = {"--benchmark"}, description = "benchmarking the throughput of all applications")
    public boolean benchmark = false;
    @Parameter(names = {"--load"}, description = "benchmarking the throughput of all applications")
    public boolean load = false;
    @Parameter(names = {"--microbenchmark"}, description = "benchmarking the throughput of all applications")
    public boolean microbenchmark = false;
    @Parameter(names = {"--worst"}, description = "worst case plan study")
    public boolean worst = false;
    @Parameter(names = {"--profile"}, description = "profiling")
    public boolean profile = false;
    @Parameter(names = {"--profile_start"}, description = "profile_start")
    public int profile_start = -1;
    @Parameter(names = {"--profile_end"}, description = "profile_end")
    public int profile_end = -1;
    @Parameter(names = {"--manual"}, description = "manual")
    public boolean manual = false;
    @Parameter(names = {"--DO"}, description = "dynamic optimization")
    public boolean DO = false;
    @Parameter(names = {"--native"}, description = "native execution")
    public boolean NAV = false;
    @Parameter(names = {"--random"}, description = "random plan")
    public boolean random = false;
    @Parameter(names = {"--roundrobin", "-RR"}, description = "roundrobin plan")
    public boolean roundrobin = false;
    @Parameter(names = {"--toff"}, description = "toff plan")
    public boolean toff = false;
    @Parameter(names = {"--percentile"}, description = "profile percentile")
    public int percentile = 50;
    @Parameter(names = {"--routing"}, description = "routing optimization")
    public boolean routing = false;
    @Parameter(names = {"--parallelism_tune", "--tune"}, description = "routing optimization")
    public boolean parallelism_tune = false;
    @Parameter(names = {"--sim", "--simulation"}, description = "simulate hardware environment")
    public boolean simulation = false;
    @Parameter(names = {"--monte"}, description = "monte carlo simulation ")
    public boolean monte = false;
    @Parameter(names = {"--num_socket"}, description = "allowed #socket")
    public int num_socket = 1;
    @Parameter(names = {"--num_cpu"}, description = "allowed #CPU cores on each socket")
    public int num_cpu = -1;
    @Parameter(names = {"--relax"}, description = "constraint relax factor")
    public double relax = 1;
    @Parameter(names = {"--repeat"}, description = "repeat execution")
    public int repeat = 1;
    @Parameter(names = {"--loop"}, description = "measure time")
    public int loop = 100000;
    @Parameter(names = {"--JVM"}, description = "JVM size in GB")
    public int JVM = 256;
    @Parameter(names = {"--THz", "-THz"}, description = "target input Hz")
    public double THz = Constants.default_sourceRate;
    @Parameter(names = {"-input"}, description = "target percentage input rate")
    public double input = 1.0;
    @Parameter(names = {"--size_tuple"}, description = "size_tuple (number of elements in state)")
    public int size_tuple = 0;
    @Parameter(names = {"-w", "--window"}, description = "window")
    public int window = 2;
    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the Brisk.topology (local mode only)")
    public int runtimeInSeconds = 30;
    @Parameter(names = {"-st"}, description = "spout.threads", required = false)
    public int sthread = 1;//default 5
    @Parameter(names = {"-sit"}, description = "sink.threads", required = false)
    public int sithread = 1;//default 10
    @Parameter(names = {"-pt"}, description = "parser.threads", required = false)
    public int pthread = 1;
    @Parameter(names = {"-tt"}, description = "parallelism", required = false)
    public int tthread = 1;
    @Parameter(names = {"--TP"}, description = "TP threads")
    public int TP = -1;// default TP threads
    @Parameter
    public List<String> parameters = Lists.newArrayList();
    @Parameter(names = {"-m", "--mode"}, description = "Mode for running the topology")
    public String mode = RUN_LOCAL;
    @Parameter(names = {"-t", "--Brisk.topology-name"}, required = false, description = "The name of the Brisk.topology")
    public String topologyName;
    @Parameter(names = {"--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;

    @Parameter(names = {"-bt"}, description = "fixed batch", required = false)
    public int batch = 100;
    @Parameter(names = {"--upperlimit"}, description = "Test upperlimit throughput")
    public boolean upperlimit = false;
    @Parameter(names = {"--verbose"}, description = "whether print execution detail")
    public boolean verbose = false;
    @Parameter(names = {"--task_type"}, description = "task_type, 0:stateless, 1:PS, 2: FS")
    public int task_type = 0;
    @Parameter(names = {"-co"}, description = "TOPOLOGY_WORKER_CHILDOPTS", required = false)
    public String CHILDOPTS = "";
    @Parameter(names = {"-log"}, description = "log property", required = false)
    public String log = "standout";
    @Parameter(names = {"--timeslice"}, description = "time slice used in spout (ms)")
    public int timeSliceLengthMs = 100;//ms
    @Parameter(names = {"--parallelism"}, description = "default parallelism setting per operator used in Flink-Storm translation")
    public int parallelism = 10;
    @Parameter(names = {"--compressRatio"}, description = "compressRatio")
    public int compressRatio = 1;
    @Parameter(names = {"--totalEventsPerBatch"}, description = "Total number of events per batch.")
    public int totalEventsPerBatch = 1;
    @Parameter(names = {"--numberOfBatches"}, description = "Total number of batches.")
    public int numberOfBatches = 1;

    @Parameter(names = {"--numberOfDLevels"}, description = "Maximum number of input data dependency levels.")
    public Integer numberOfDLevels = 4;
    @Parameter(names = {"--iterationNumber"}, description = "Number of dependency levels.")
    public Integer iterationNumber = 0;
    @Parameter(names = {"--scheduler"}, description = "Scheduler for TStream.")
    public String scheduler = "BL";
    @Parameter(names = {"--fanoutDist"}, description = "Fanout rate distribution scheme. [uniform, zipfinv, zipf, zipfcenter]")
    public String fanoutDist = "uniform";
    @Parameter(names = {"--idGenType"}, description = "State ids distribution scheme.[uniform, normal]")
    public String idGenType = "uniform";

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

    public void configuration(HashMap<String, Object> config) {
        config.put("Fault_tolerance", enable_fault_tolerance);
        config.put("disable_pushdown", disable_pushdown);
        config.put("common", application);
        config.put("ratio_of_multi_partition", ratio_of_multi_partition);
        config.put("number_partitions", number_partitions);
        config.put("compressRatio", compressRatio);
        config.put("parallelism", parallelism);
        config.put("load", load);
        config.put("toff", toff);
        config.put("roundrobin", roundrobin);
        config.put("microbenchmark", microbenchmark);
        config.put("percentile", percentile);
        config.put("machine", machine);
        config.put("random", random);
        config.put("parallelism_tune", parallelism_tune);
        config.put("input", input);
        config.put("pthread", pthread);
        config.put("repeat", repeat);
        config.put("plan", plan);
        config.put("profile", profile);
        config.put("profile_start", profile_start);
        config.put("profile_end", profile_end);
        config.put("totalEventsPerBatch", totalEventsPerBatch);
        config.put("numberOfBatches", numberOfBatches);
        config.put("rootFilePath", rootPath);
        config.put("scheduler", scheduler);
        config.put("fanoutDist", fanoutDist);
        config.put("idGenType", idGenType);
        config.put("numberOfDLevels", numberOfDLevels);

        if (num_socket != -1) {
            config.put("num_socket", num_socket);
        } else {
//            config.put("num_socket", OsUtils.totalSockets());
            config.put("num_socket", 1);
        }
        if (num_cpu != -1) {
            config.put("num_cpu", num_cpu);
        } else {
//            config.put("num_cpu", OsUtils.TotalCores() / OsUtils.totalSockets());
            config.put("num_cpu", OsUtils.TotalCores() / 1);
        }
        config.put("transaction", enable_transaction);

        if (CCOption == 4)//S-Store enabled.
            config.put("partition", true);
        else
            config.put("partition", enable_partition);
        config.put("measure", enable_measurement);
        config.put("checkpoint", checkpoint_interval);
        if (TP != -1)
            config.put("TP", TP);
        else
            config.put("TP", tthread);
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
        config.put("relax", relax);
        config.put("monte", monte);
        config.put("DO", DO);
        config.put("NAV", NAV);
        config.put("routing", routing);
        config.put("simulation", simulation);
        config.put("loop", loop);
        config.put("JVM", JVM);
        if (tthread != -1) {
            config.put("tthread", tthread);
        } else {
            config.put("tthread", 1);
        }
        // load default configuration
        config.put("targetHz", THz);
        config.put("timeSliceLengthMs", timeSliceLengthMs);
        if (batch != -1) {
            config.put("batch", batch);
        }
        config.put("metrics.output", metric_path);
        config.put("runtimeInSeconds", runtimeInSeconds);
        config.put("size_tuple", size_tuple);
        config.put("task_type", task_type);
        config.put("window", window);
        config.put("mode", mode);
        config.put("verbose", verbose);
        config.put("upperlimit", upperlimit);
        config.put("worst", worst);
        System.setProperty("my.log", metric_path);
    }
}
