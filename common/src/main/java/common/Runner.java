package common;

import com.beust.jcommander.Parameter;
import common.collections.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import static common.CONTROL.enable_log;

public abstract class Runner implements IRunner {
    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);
    protected static String CFG_PATH = null;
    /**
     * Workload Specific Parameters.
     */
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed")
//    public String application = "StreamLedger";
//    public String application = "GrepSum";
    //public String application = "OnlineBiding";
    //public String application = "TollProcessing";
    public String application = "EventDetection";
    @Parameter(names = {"-t", "--topology-name"}, required = false, description = "The name of the application")
    public String topologyName;
    @Parameter(names = {"--COMPUTE_COMPLEXITY"}, description = "COMPUTE_COMPLEXITY per event")
    public int COMPUTE_COMPLEXITY = 0;// 1, 10, 100
    @Parameter(names = {"--POST_COMPUTE"}, description = "POST COMPUTE_COMPLEXITY per event")
    public int POST_COMPUTE = 0;// 1, 10, 100
    @Parameter(names = {"--NUM_ITEMS"}, description = "NUM_ITEMS in DB.")
    public int NUM_ITEMS = 100_000;//
//    public int NUM_ITEMS = 500;//
    @Parameter(names = {"--NUM_ACCESS"}, description = "Number of state access per transaction")
    public int NUM_ACCESS = 5;//
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
    @Parameter(names = {"--tthread"}, description = "total execution threads")
    public int tthread = 4;// default total execution threads
    @Parameter(names = {"--CCOption"}, description = "Selecting different concurrency control options.")
    public int CCOption = CCOption_MorphStream;
//    public int CCOption = CCOption_SStore;
//    public int CCOption = CCOption_LOCK;
    @Parameter(names = {"--partition"}, description = "Partitioning database. It must be enabled for S-Store scheme and it is optional for TStream scheme.")
    public boolean enable_partition = false;
    @Parameter(names = {"--scheduler"}, description = "Scheduler for TStream.")
    public String scheduler = "OP_BFS_A";
    //public String scheduler = "OG_BFS_A";
//    public String scheduler = "OG_DFS";
//    public String scheduler = "OG_DFS_A";
//    public String scheduler = "OG_NS";
//    public String scheduler = "OG_NS_A";
//    public String scheduler = "OP_NS";
//    public String scheduler = "OP_NS_A";
//    public String scheduler = "OP_BFS";
//    public String scheduler = "OP_BFS_A";
//    public String scheduler = "OP_DFS";
//    public String scheduler = "OP_DFS_A";
//    public String scheduler = "TStream";
    @Parameter(names = {"--fanoutDist"}, description = "Fanout rate distribution scheme. [uniform, zipfinv, zipf, zipfcenter]")
    public String fanoutDist = "uniform";
    @Parameter(names = {"--idGenType"}, description = "State ids distribution scheme.[uniform, normal]")
    public String idGenType = "uniform";

    /**
     * Dynamic Scheduler
     */
    @Parameter(names = {"--isRuntime"}, description = "Collect runtime information")
    public boolean isRuntime = false;
    @Parameter(names = {"--isDynamic"}, description = "Dynamic Workload")
    public int isDynamic = 0;
    @Parameter(names = {"--schedulerPool"}, description = "Schedulers in the SchedulerPool[OG_DFS,OP_DFS]")
    public String schedulerPools = "";
    @Parameter(names = {"--defaultScheduler"}, description = "Default scheduler")
    public String defaultScheduler = "";
    @Parameter(names = {"--bottomLine"}, description = "BottomLine for(TD,LD,PD,SUM,VDD,R_of_A)")
    public String bottomLine = "";
    @Parameter(names = {"--WorkloadConfig"}, description = "WorkloadConfigs(TD,LD,PD,VDD,R_of_A,isCD,isCC,markId)")
    public String WorkloadConfig = "";

    /**
     * Dynamic workload
     */
    @Parameter(names = {"--workloadType"}, description = "which type of dynamic workload")
    public String  workloadType = "default";
    @Parameter(names = {"--shiftRate"}, description = "control the rate of the workload shift")
    public int shiftRate  = 1;
    @Parameter(names = {"--phaseNum"}, description = "How many phases")
    public int phaseNum  =1;

    /**
     * Scheduler for group
     */
    @Parameter(names = {"--isGroup"}, description = "Group for TP")
    public int isGroup = 0;
    @Parameter(names = {"--groupNum"}, description = "How many groups")
    public int groupNum = 1;
    @Parameter(names = {"--SchedulersForGroup"}, description = "Schedulers [OG_DFS,OP_DFS]")
    public String SchedulersForGroup = "";
    @Parameter(names = {"--skewGroup"}, description = "skew for groups")
    public String skewGroup = "0,100";
    @Parameter(names = {"--high_abort_ratio"}, description = "abort ratio for groups")
    public Integer Ratio_of_Transaction_Aborts_Highest = 0;

    /**
     * Benchmarking Specific Parameters.
     */
    @Parameter(names = {"--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr;
    @Parameter(names = {"--measure"}, description = "enable measurement")
    public boolean enable_measurement = false;
    @Parameter(names = {"--rootFilePath"}, description = "Root path for data files.")
    public String rootPath = System.getProperty("user.home") + OsUtils.OS_wrapper("data");
    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = rootPath + OsUtils.OS_wrapper("metric_output");
    @Parameter(names = {"--machine"}, description = "which machine to use? 0 (default): a simple one-socket machine with four cores. Add your machine specification accordingly and select them by change this specification")
    public int machine = 0;
    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the Brisk.topology (local mode only)")
    public int runtimeInSeconds = 30;
    @Parameter(names = {"--verbose"}, description = "whether print execution detail")
    public boolean verbose = false;

    /**
     * generator parameters
     */
    @Parameter(names = {"--checkpoint_interval"}, description = "checkpoint interval (#tuples)")
    public int checkpoint_interval = 2500;//checkpoint per thread.
    @Parameter(names = {"--generator"}, description = "Generator for TStream.")
    public String generator = "TPGGenerator";
//    public String generator = "OCGenerator";
    @Parameter(names = {"--totalEvents"}, description = "Total number of events to process.")
//    public int totalEvents = 10000;
    public int totalEvents = 100;

    @Parameter(names = {"--deposit_ratio"}, description = "Ratio of deposit for SL.")
    public Integer Ratio_Of_Deposit = 25;

    @Parameter(names = {"--buying_ratio"}, description = "Ratio of buying for OB.")
    public Integer Ratio_Of_Buying = 25;

    @Parameter(names = {"--key_skewness"}, description = "State access skewness.")
    public Integer State_Access_Skewness = 20;

    @Parameter(names = {"--multiple_ratio"}, description = "State access skewness.")
    public Integer Ratio_of_Multiple_State_Access = 100;

    @Parameter(names = {"--overlap_ratio"}, description = "Ratio of overlapped keys.")
    public Integer Ratio_of_Overlapped_Keys = 10;

    @Parameter(names = {"--abort_ratio"}, description = "Ratio of transaction aborts.")
    public Integer Ratio_of_Transaction_Aborts = 0;

    @Parameter(names = {"--txn_length"}, description = "Transaction Length.")
    public Integer Transaction_Length = 1;

    @Parameter(names = {"--numberOfDLevels"}, description = "Maximum number of input data dependency levels.")
    public Integer numberOfDLevels = 1024;

    @Parameter(names = {"--isCyclic"}, description = "isCyclic of generated OC.")
    public int isCyclic = 0;

    @Parameter(names = {"--complexity"}, description = "Dummy UDF complexity for state access process.")
    public Integer complexity = 0;

    public Runner() {
        CFG_PATH = "/config/%s.properties";
        if (enable_log) LOG.info(String.format("Metric folder path %s.", metric_path));
    }

    public Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is = Runner.class.getResourceAsStream(filename);
        properties.load(is);
        assert is != null;
        is.close();
        return properties;
    }

    public void initializeCfg(HashMap<String, Object> config) {
        config.put("queue_size", queue_size);
        config.put("common", application);
        config.put("ratio_of_multi_partition", ratio_of_multi_partition);
        config.put("number_partitions", number_partitions);
        config.put("machine", machine);
        config.put("totalEvents", totalEvents);
        config.put("rootFilePath", rootPath);
        config.put("generator", generator);
        config.put("fanoutDist", fanoutDist);
        config.put("idGenType", idGenType);

        if (application.equals("OnlineBiding")){
            config.put("Ratio_Of_Buying", Ratio_Of_Buying);
        }else if (application.equals("StreamLedger")) {
            config.put("Ratio_Of_Deposit", Ratio_Of_Deposit);
        } else {}
        config.put("State_Access_Skewness", State_Access_Skewness);
        config.put("Ratio_of_Overlapped_Keys", Ratio_of_Overlapped_Keys);
        config.put("Ratio_of_Transaction_Aborts", Ratio_of_Transaction_Aborts);
        config.put("Ratio_of_Multiple_State_Access", Ratio_of_Multiple_State_Access);
        config.put("Transaction_Length", Transaction_Length);

        config.put("numberOfDLevels", numberOfDLevels);
        if (isCyclic == 1) {
            config.put("isCyclic", true);
        } else {
            config.put("isCyclic", false);
        }
        config.put("complexity", complexity);

        if (CCOption == 4)//S-Store enabled.
            config.put("partition", true);
        else
            config.put("partition", enable_partition);
        config.put("measure", enable_measurement);
        config.put("tthread", tthread);

        config.put("checkpoint", checkpoint_interval);

        assert totalEvents % tthread == 0;

        config.put("COMPUTE_COMPLEXITY", COMPUTE_COMPLEXITY);
        config.put("POST_COMPUTE", POST_COMPUTE);
        config.put("NUM_ACCESS", NUM_ACCESS);
        config.put("NUM_ITEMS", NUM_ITEMS);
        config.put("CCOption", CCOption);
        config.put("linked", linked);
        config.put("shared", shared);
        config.put("ratio_of_read", ratio_of_read);
        config.put("theta", theta);

        if (batch != -1) {
            config.put("batch", batch);
        }
        config.put("metrics.output", metric_path);
        config.put("runtimeInSeconds", runtimeInSeconds);
        config.put("size_tuple", size_tuple);
        config.put("verbose", verbose);

        config.put("application",application);

        String phaseType[]=workloadType.split(",");
        switch(application) {
            case "StreamLedger" :
                //bottomLine = "300,3000,500,1200,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                bottomLine = 0.2*checkpoint_interval+","+2*checkpoint_interval+","+0.3*checkpoint_interval+","+0.8*checkpoint_interval+","+"0.3,0.7";//TD,LD,PD,SUM,VDD,R_of_A
                phaseNum = shiftRate * phaseType.length;
                break;
            case "OnlineBiding" :
                bottomLine = "500,5000,1,6000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                schedulerPools = "OG_BFS_A,OG_NS_A,OG_NS";
                defaultScheduler = "OG_BFS_A";
                phaseNum = shiftRate * phaseType.length;
                break;
            case "GrepSum" :
                bottomLine = "500,5000,6500,3000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                schedulerPools = "OP_NS_A,OG_BFS_A,OP_NS,OP_NS_A";
                defaultScheduler = "OP_NS_A";
                phaseNum = shiftRate * phaseType.length;
                break;
            case "TollProcessing" :
                phaseNum = shiftRate * 1;
                defaultScheduler = "OG_BFS_A";
                break;
            case "EventDetection" :
                //TODO: Add Conf settings to ED
                bottomLine = "500,5000,6500,3000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                schedulerPools = "OP_NS_A,OG_BFS_A,OP_NS,OP_NS_A";
                defaultScheduler = "OP_NS_A";
                phaseNum = shiftRate * phaseType.length;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + application);
        }
        /* Dynamic switch scheduler*/
        if (isDynamic == 1) {
            config.put("isDynamic", true);
            //config.put("totalEvents", phaseNum * tthread * checkpoint_interval);
            config.put("schedulersPool", schedulerPools);
            config.put("defaultScheduler", defaultScheduler);
            config.put("scheduler", defaultScheduler);
            config.put("isRuntime", isRuntime);
            config.put("bottomLine", bottomLine);
            config.put("WorkloadConfig", WorkloadConfig);
        } else {
            config.put("isDynamic", false);
            config.put("scheduler", scheduler);
        }


        /* Dynamic Workload Configuration*/
        config.put("workloadType", workloadType);
        config.put("shiftRate", shiftRate);
        config.put("phaseNum", phaseNum);

        /* Group scheduler*/
        if (isGroup == 1) {
            config.put("isGroup", true);
            config.put("groupNum",groupNum);
            config.put("SchedulersForGroup",SchedulersForGroup);
            config.put("totalEvents",phaseNum * tthread * checkpoint_interval);
            config.put("skewGroup",skewGroup);
            config.put("Ratio_of_Transaction_Aborts_Highest",Ratio_of_Transaction_Aborts_Highest);
        } else {
            config.put("isGroup", false);
            config.put("groupNum",1);
        }

        System.setProperty("my.log", metric_path);
    }
}
