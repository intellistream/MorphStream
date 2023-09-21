package intellistream.morphstream.api.launcher;

import com.beust.jcommander.Parameter;
import intellistream.morphstream.common.io.Enums.platform.HP_Machine;
import intellistream.morphstream.common.io.Enums.platform.HUAWEI_Machine;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.configuration.Constants;
import intellistream.morphstream.engine.txn.durability.struct.FaultToleranceRelax;
import intellistream.morphstream.engine.txn.scheduler.struct.OperationChainCommon;
import intellistream.morphstream.util.AppConfig;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.engine.txn.content.LVTStreamContent.LVTSTREAM_CONTENT;
import static intellistream.morphstream.engine.txn.content.LWMContentImpl.LWM_CONTENT;
import static intellistream.morphstream.engine.txn.content.LockContentImpl.LOCK_CONTENT;
import static intellistream.morphstream.engine.txn.content.SStoreContentImpl.SSTORE_CONTENT;
import static intellistream.morphstream.engine.txn.content.TStreamContent.T_STREAMCONTENT;
import static intellistream.morphstream.engine.txn.content.common.ContentCommon.content_type;
import static intellistream.morphstream.engine.txn.content.common.ContentCommon.loggingRecord_type;
import static intellistream.morphstream.util.FaultToleranceConstants.*;
import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_command;

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
    @Parameter(names = {"--valueNamesForTables"}, description = "value names for each table, split by ,")
    public String valueNamesForTables = "value1,value2";
    //Input event configurations
    @Parameter(names = {"--inputFileType"}, description = "input file type, [txt, csv, json]")
    public int inputFileType = 0;
    @Parameter(names = {"--inputFile"}, description = "path of input file ")
    public String inputFile = "/Users/curryzjj/hair-loss/MorphStream/Benchmark/input/event.txt";
    @Parameter(names = {"--inputFileName"}, description = "input file name")
    public String inputFileName = "events.txt";
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
    @Parameter(names = {"conditionNameForEvents"}, description = "condition names for each type of event, split by ;")
    public String conditionNameForEvents = "c1,c2;c1,c2";
    @Parameter(names = {"--eventRatio"}, description = "event ratio for each type of event, split by ,")
    public String eventRatio = "0.5,0.5";
    @Parameter(names = {"--ratioOfMultiPartitionTransactionsForEvents"}, description = "ratio of multi partition transactions for each type of event, split by ,")
    public String ratioOfMultiPartitionTransactionsForEvents = "0.5,0.5";
    @Parameter(names = {"--stateAccessSkewnessForEvents"}, description = "state access skewness for each types of event, split by ,")
    public String stateAccessSkewnessForEvents = "0.5,0.5";

    //Client
    @Parameter(names = {"--clientClassName"}, description = "Client class name, used for UDF Reflection")
    public String clientClassName = "Client";

    public JCommanderHandler() {}

    public Properties loadProperties(String filename) throws IOException {
        Properties properties = new Properties();
        InputStream is = JCommanderHandler.class.getResourceAsStream(filename);
        properties.load(is);
        assert is != null;
        is.close();
        return properties;
    }

    public void initializeCfg(Configuration config) {
        assert config != null;
        config.put("clientClassName", clientClassName);
        //Database configuration
        config.put("tableNames", tableNames);
        String[] tableNameString = tableNames.split(",");
        for (int i = 0; i < tableNameString.length; i ++) {
            config.put(tableNameString[i] + "_num_items", Integer.parseInt(numberItemsForTables.split(",")[i]));
            config.put(tableNameString[i] + "_key_data_types", keyDataTypesForTables.split(",")[i]);
            config.put(tableNameString[i] + "_value_data_types", valueDataTypesForTables.split(",")[i]);
            config.put(tableNameString[i] + "_value_names", valueNamesForTables.split(",")[i]);
        }
        //Input events configuration
        config.put("inputFileType", inputFileType);
        config.put("inputFile", inputFile);
        config.put("inputFileName", inputFileName);
        config.put("totalEvents", totalEvents);
        config.put("workloadType", workloadType);
        config.put("eventTypes", eventTypes);
        String[] eventTypeString = eventTypes.split(";");
        for (int i = 0; i < eventTypeString.length; i ++) {
            config.put(eventTypeString[i] + "_tables", tableNameForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_key_number", keyNumberForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_values", valueNameForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_conditions", conditionNameForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_event_ratio", eventRatio.split(",")[i]);
            config.put(eventTypeString[i] + "_ratio_of_multi_partition_transactions", ratioOfMultiPartitionTransactionsForEvents.split(",")[i]);
            config.put(eventTypeString[i] + "_state_access_skewness", stateAccessSkewnessForEvents.split(",")[i]);
        }

    }
    private void configSystem(Configuration config) {
        //Add platform specific configurations
        //Configure Database Content Type
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
        //Configure Fault Tolerance
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
        //AppConfig
        AppConfig.complexity = config.getInt("complexity", 100000);
        AppConfig.windowSize = config.getInt("windowSize", 1024);
        AppConfig.isCyclic = config.getBoolean("isCyclic", true);
        //Add Metric Configuration
        //if (CONTROL.enable_shared_state) {
        //    Metrics.COMPUTE_COMPLEXITY = conf.getInt("COMPUTE_COMPLEXITY");
        //    Metrics.POST_COMPUTE_COMPLEXITY = conf.getInt("POST_COMPUTE");
        //    Metrics.NUM_ACCESSES = conf.getInt("NUM_ACCESS");
        //    Metrics.NUM_ITEMS = conf.getInt("NUM_ITEMS");
        //    Metrics.H2_SIZE = Metrics.NUM_ITEMS / conf.getInt("tthread");
        //}
    }
}
