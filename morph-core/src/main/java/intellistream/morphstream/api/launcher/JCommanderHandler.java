package intellistream.morphstream.api.launcher;

import com.beust.jcommander.Parameter;
import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class JCommanderHandler {
    private static final Logger LOG = LoggerFactory.getLogger(JCommanderHandler.class);
    /**
     * Workload Specific Parameters.
     */
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed")
    public String application = "StreamLedger";
    //public String application = "SHJ";
    //public String application = "GrepSum";
    //public String application = "WindowedGrepSum";
    //public String application = "OnlineBiding";
    //public String application = "TollProcessing";
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
    public int CCOption = Constants.CCOption_MorphStream;
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
    @Parameter(names = {"--shiftRate"}, description = "control the rate of the workload shift")
    public int shiftRate = 1;
    @Parameter(names = {"--phaseNum"}, description = "How many phases")
    public int phaseNum = 1;

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

    @Parameter(names = {"--window_trigger_period"}, description = "Ratio of window events in the transaction.")
    public Integer Period_of_Window_Reads = 1024;

    @Parameter(names = {"--window_size"}, description = "Window Size for the window operations.")
    public Integer windowSize = 1024;
    @Parameter(names = {"--nondeterministic_ratio"}, description = "Ratio_of_Non_Deterministic_State_Access.(10 -> 0.01%)")
    public Integer Ratio_of_Non_Deterministic_State_Access = 0;

    @Parameter(names = {"--txn_length"}, description = "Transaction Length.")
    public Integer Transaction_Length = 1;

    @Parameter(names = {"--numberOfDLevels"}, description = "Maximum number of input data dependency levels.")
    public Integer numberOfDLevels = 1024;

    @Parameter(names = {"--isCyclic"}, description = "isCyclic of generated OC.")
    public int isCyclic = 0;

    @Parameter(names = {"--complexity"}, description = "Dummy UDF complexity for state access process.")
    public Integer complexity = 0;

    /**
     * Evaluation parameters
     */
    @Parameter(names = {"--multicoreEvaluation"}, description = "Evaluation the multicoreScalability. Default to be false.")
    public Integer multicoreEvaluation = 0;
    @Parameter(names = {"--maxThreads"}, description = "Evaluation the multicoreScalability. The max number of threads.")
    public Integer maxThreads = 24;
    //SystemOverhead
    @Parameter(names = {"--cleanUp"}, description = "Whether to clean temporal objects. Default to be false.")
    public Integer cleanUp = 0;

    /* Fault Tolerance */
    @Parameter(names = {"--FTOption"}, description = "Fault tolerance mechanisms: 0 for noFT, 1 for checkpoint, 2 for wal, ")
    public Integer FTOption = 0;
    @Parameter(names = {"--compressionAlg"}, description = "compression Alg: ")
    public String compressionAlg = "None";
    @Parameter(names = {"--snapshotInterval"}, description = "Snapshot interval ")
    public Integer snapshotInterval = 0;
    @Parameter(names = {"--arrivalRate"}, description = "Arrival rate of event, 200k/s ")
    public Integer arrivalRate = 200;
    @Parameter(names = {"--arrivalControl"}, description = "Arrival control of event ")
    public Integer arrivalControl = 0;
    @Parameter(names = {"--isRecovery"}, description = "Whether to recover or not")
    public Integer isRecovery = 0;
    @Parameter(names = {"--isFailure"}, description = "Whether to emulate system failure ")
    public Integer isFailure = 0;
    @Parameter(names = {"--failureTime"}, description = "When to emulate system failure (in ms)")
    public Integer failureTime = 3000;
    @Parameter(names = {"--measureInterval"}, description = "Interval to compute throughput, default to be 100ms")
    public Double measureInterval = 100.0;
    //Fault tolerance relax
    @Parameter(names = {"--isHistoryView"}, description = "Whether to dependency inspection or not")
    public Integer isHistoryView = 1;
    @Parameter(names = {"--isAbortPushDown"}, description = "Whether to abort push down or not")
    public Integer isAbortPushDown = 1;
    @Parameter(names = {"--isTaskPlacing"}, description = "Whether to task placing or not")
    public Integer isTaskPlacing = 1;
    @Parameter(names = {"--isSelectiveLogging"}, description = "Whether to selective logging or not")
    public Integer isSelectiveLogging = 0;
    @Parameter(names = {"--maxItr"}, description = "Max itr for graph partition alg: ")
    public int maxItr = 10;

    //Database configurations
    @Parameter(names = {"--tableNames"}, description = "String of table names, split by ,")
    public String tableNames = "table1,table2";
    @Parameter(names = {"--numberItemsForTables"}, description = "number of items for each table, split by ,")
    public String numberItemsForTables = "1000000,1000000";
    @Parameter(names = {"--keyDataTypesForTables"}, description = "key data types for each table, split by ,")
    public String keyDataTypesForTables = "string,string";
    @Parameter(names = {"--valueDataTypesForTables"}, description = "value data types for each table, split by ,")
    public String valueDataTypesForTables = "int,int";
    //Input event configurations
    @Parameter(names = {"--totalEvents"}, description = "Total number of events to process.")
    public int totalEvents = 10000;
    @Parameter(names = {"--workloadType"}, description = "which type of dynamic workload")
    public String workloadType = "default";
    @Parameter(names = {"--eventTypes"}, description = "String of event types, split by ,")
    public String eventTypes = "event1,event2";
    @Parameter(names = {"--tableNameForEvents"}, description = "table names for each type of event, split by ;")
    public String tableNameForEvents = "table1,table2;table1,table2";
    @Parameter(names = {"keyNumberForEvents"}, description = "number of keys for each type of event, split by ;")
    public String keyNumberForEvents = "2,2;2,2";
    @Parameter(names = {"valueNameForEvents"}, description = "value names for each type of event, split by ;")
    public String valueNameForEvents = "v1,v2;v1,v2";
    @Parameter(names = {"--eventRatio"}, description = "event ratio for each type of event, split by ,")
    public String eventRatio = "0.5,0.5";
    @Parameter(names = {"--ratioOfMultiPartitionTransactionsForEvents"}, description = "ratio of multi partition transactions for each type of event, split by ,")
    public String ratioOfMultiPartitionTransactionsForEvents = "0.5,0.5";
    @Parameter(names = {"--stateAccessSkewnessForEvents"}, description = "state access skewness for each types of event, split by ,")
    public String stateAccessSkewnessForEvents = "0.5,0.5";

    public JCommanderHandler() {
        if (enable_log) LOG.info(String.format("Metric folder path %s.", metric_path));
    }

    public Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is = JCommanderHandler.class.getResourceAsStream(filename);
        properties.load(is);
        assert is != null;
        is.close();
        return properties;
    }

    public void initializeCfg(HashMap<String, Object> config) {
        //Input events configuration
        config.put("tableNames", tableNames);
        String[] tableNameString = tableNames.split(",");
        for (int i = 0; i < tableNameString.length; i ++) {
            config.put(tableNameString[i] + "_num_items", Integer.parseInt(numberItemsForTables.split(",")[i]));
            config.put(tableNameString[i] + "_key_data_types", keyDataTypesForTables.split(",")[i]);
            config.put(tableNameString[i] + "_value_data_types", valueDataTypesForTables.split(",")[i]);
        }
        //Database configuration
        config.put("totalEvents", totalEvents);
        config.put("workloadType", workloadType);
        config.put("eventTypes", eventTypes);
        String[] eventTypeString = eventTypes.split(";");
        for (int i = 0; i < eventTypeString.length; i ++) {
            config.put(eventTypeString[i] + "_tables", tableNameForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_key_number", keyNumberForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_values", valueNameForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_event_ratio", eventRatio.split(",")[i]);
            config.put(eventTypeString[i] + "_ratio_of_multi_partition_transactions", ratioOfMultiPartitionTransactionsForEvents.split(",")[i]);
            config.put(eventTypeString[i] + "_state_access_skewness", stateAccessSkewnessForEvents.split(",")[i]);
        }

        //Old Version
        config.put("queue_size", queue_size);
        config.put("app_name", application);
        config.put("ratio_of_multi_partition", ratio_of_multi_partition);
        config.put("number_partitions", number_partitions);
        config.put("machine", machine);
        config.put("rootFilePath", rootPath);
        config.put("generator", generator);
        config.put("fanoutDist", fanoutDist);
        config.put("idGenType", idGenType);

        if (application.equals("OnlineBiding")) {
            config.put("Ratio_Of_Buying", Ratio_Of_Buying);
        } else {
            config.put("Ratio_Of_Deposit", Ratio_Of_Deposit);
        }
        config.put("State_Access_Skewness", State_Access_Skewness);
        config.put("Ratio_of_Overlapped_Keys", Ratio_of_Overlapped_Keys);
        config.put("Ratio_of_Transaction_Aborts", Ratio_of_Transaction_Aborts);
        config.put("Period_of_Window_Reads", Period_of_Window_Reads);
        config.put("Ratio_of_Multiple_State_Access", Ratio_of_Multiple_State_Access);
        config.put("Transaction_Length", Transaction_Length);
        config.put("Ratio_of_Non_Deterministic_State_Access", Ratio_of_Non_Deterministic_State_Access);

        config.put("numberOfDLevels", numberOfDLevels);
        if (isCyclic == 1) {
            config.put("isCyclic", true);
        } else {
            config.put("isCyclic", false);
        }
        config.put("complexity", complexity);
        config.put("windowSize", windowSize);

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

        String[] phaseType = workloadType.split(",");
        switch (application) {
            case "StreamLedger":
                //bottomLine = "300,3000,500,1200,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                bottomLine = 0.2 * checkpoint_interval + "," + 2 * checkpoint_interval + "," + 0.3 * checkpoint_interval + "," + 0.8 * checkpoint_interval + "," + "0.3,0.7";//TD,LD,PD,SUM,VDD,R_of_A
                phaseNum = shiftRate * phaseType.length;
                break;
            case "OnlineBiding":
                bottomLine = "500,5000,1,6000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                schedulerPools = "OG_BFS_A,OG_NS_A,OG_NS";
                defaultScheduler = "OG_BFS_A";
                phaseNum = shiftRate * phaseType.length;
                break;
            case "GrepSum":
                bottomLine = "500,5000,6500,3000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                schedulerPools = "OP_NS_A,OG_BFS_A,OP_NS,OP_NS_A";
                defaultScheduler = "OP_NS_A";
                phaseNum = shiftRate * phaseType.length;
                break;
            case "TollProcessing":
                phaseNum = shiftRate;
                defaultScheduler = "OG_BFS_A";
                break;
            case "WindowedGrepSum":
                bottomLine = "500,5000,6500,3000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                schedulerPools = "OP_NS_A,OG_BFS_A,OP_NS,OP_NS_A";
                defaultScheduler = "OP_NS_A";
                phaseNum = shiftRate * phaseType.length;
                break;
            case "SHJ":
                bottomLine = "500,5000,6500,3000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
                schedulerPools = "OP_NS_A,OG_BFS_A,OP_NS,OP_NS_A";
                defaultScheduler = "OP_NS_A";
                phaseNum = shiftRate * phaseType.length;
                break;
            case "NonGrepSum":
                //TODO: Add Conf settings to NonGrepSum
                bottomLine = "500,5000,6500,3000,0.2,0.2";//TD,LD,PD,SUM,VDD,R_of_A
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
            config.put("groupNum", groupNum);
            config.put("SchedulersForGroup", SchedulersForGroup);
            config.put("totalEvents", phaseNum * tthread * checkpoint_interval);
            config.put("skewGroup", skewGroup);
            config.put("Ratio_of_Transaction_Aborts_Highest", Ratio_of_Transaction_Aborts_Highest);
        } else {
            config.put("isGroup", false);
            config.put("groupNum", 1);
        }

        /* Evaluation Configuration */
        if (multicoreEvaluation == 0) {
            config.put("multicoreEvaluation", false);
        } else {
            config.put("multicoreEvaluation", true);
        }
        config.put("maxThreads", maxThreads);

        if (cleanUp == 0) {
            config.put("cleanUp", false);
        } else {
            config.put("cleanUp", true);
        }
        /* Fault Tolerance */
        config.put("FTOption", FTOption);
        config.put("parallelNum", tthread);
        config.put("compressionAlg", compressionAlg);
        config.put("snapshotInterval", snapshotInterval);
        config.put("arrivalRate", arrivalRate);
        config.put("failureTime", failureTime);
        config.put("measureInterval", measureInterval);
        config.put("maxItr", maxItr);
        if (arrivalControl == 0) {
            config.put("arrivalControl", false);
        } else {
            config.put("arrivalControl", true);
        }
        if (isRecovery == 0) {
            config.put("isRecovery", false);
        } else {
            config.put("isRecovery", true);
        }
        if (isFailure == 0) {
            config.put("isFailure", false);
        } else {
            config.put("isFailure", true);
        }
        /* Fault Tolerance Relax */
        if (isHistoryView == 0) {
            config.put("isHistoryView", false);
        } else {
            config.put("isHistoryView", true);
        }
        if (isAbortPushDown == 0) {
            config.put("isAbortPushDown", false);
        } else {
            config.put("isAbortPushDown", true);
        }
        if (isTaskPlacing == 0) {
            config.put("isTaskPlacing", false);
        } else {
            config.put("isTaskPlacing", true);
        }
        if (isSelectiveLogging == 0) {
            config.put("isSelectiveLogging", false);
        } else {
            config.put("isSelectiveLogging", true);
        }

        System.setProperty("my.log", metric_path);
    }

}
