package intellistream.morphstream.api.launcher;

import com.beust.jcommander.Parameter;
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
import java.util.Properties;

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
     * System configurations
     */
    @Parameter(names = {"--CCOption"}, description = "Selecting different concurrency control options.")
    public int CCOption = Constants.CCOption_MorphStream;
    //    public int CCOption = CCOption_SStore;
//    public int CCOption = CCOption_LOCK;
    @Parameter(names = {"--generator"}, description = "Generator for TStream.")
    public String generator = "TPGGenerator";
    //    public String generator = "OCGenerator";
    @Parameter(names = {"--linked"}, description = "Communication Queue as Linked List or Array (default).")
    public boolean linked = false;
    @Parameter(names = {"--shared"}, description = "Communication Queue  is shared (default) by multi producers.")
    public boolean shared = true;
    @Parameter(names = {"--partition"}, description = "Partitioning database. It must be enabled for S-Store scheme and it is optional for TStream scheme.")
    public boolean enable_partition = false;
    @Parameter(names = {"-bt"}, description = "Batch Emit.", required = false)
    public int batch = 1;
    @Parameter(names = {"--fanoutDist"}, description = "Fanout rate distribution scheme. [uniform, zipfinv, zipf, zipfcenter]")
    public String fanoutDist = "uniform";
    @Parameter(names = {"--idGenType"}, description = "State ids distribution scheme.[uniform, normal]")
    public String idGenType = "uniform";
    @Parameter(names = {"--isCyclic"}, description = "isCyclic of generated OC.")
    public int isCyclic = 0;
    @Parameter(names = {"--ratio_of_multi_partition"}, description = "ratio_of_multi_partition")
    public double ratio_of_multi_partition = 0; //<=1
    @Parameter(names = {"--number_partitions"}, description = "number_partitions")
    public int number_partitions = 3;
    @Parameter(names = {"--theta"}, description = "theta")
    public double theta = 0.6; //0.6==medium contention
    @Parameter(names = {"--size_tuple"}, description = "size_tuple (number of elements in state)")
    public int size_tuple = 0;
    @Parameter(names = {"-queue_size"}, description = "Output queue size limit.", required = false)
    public int queue_size = 10000;
    @Parameter(names = {"-t", "--topology-name"}, required = false, description = "The name of the application")
    public String topologyName;



    /**
     * Scheduling configurations
     */
    @Parameter(names = {"--scheduler"}, description = "Scheduler for TStream.")
    public String scheduler = "OP_NS";
//    public String scheduler = "OP_BFS";
//    public String scheduler = "OP_DFS";
//    public String scheduler = "TStream";
    @Parameter(names = {"--isRuntime"}, description = "Collect runtime information")
    public boolean isRuntime = false;
    @Parameter(names = {"--isDynamic"}, description = "Dynamic Workload")
    public int isDynamic = 0;
    @Parameter(names = {"--useNativeLib"}, description = "Use native library declared in c/c++")
    public boolean useNativeLib = true;
    @Parameter(names = {"--schedulerPool"}, description = "Schedulers in the SchedulerPool[OG_DFS,OP_DFS]")
    public String schedulerPools = "OP_BFS_A,OP_BFS,OP_NS_A,OP_NS";
    @Parameter(names = {"--defaultScheduler"}, description = "Default scheduler")
    public String defaultScheduler = "OP_BFS_A";
    @Parameter(names = {"--bottomLine"}, description = "BottomLine for(TD,LD,PD,SUM,VDD,R_of_A)")
    public String bottomLine = "2,1,1,2,0.5,0.5"; //threshold for each TD=2/LD=1/PD=1/SUM(TD+LD)=2/VDD=0.5/R_of_A=0.5 ... based on this to switch between schedulers
    @Parameter(names = {"--WorkloadConfig"}, description = "WorkloadConfigs(TD,LD,PD,VDD,R_of_A,isCD,isCC,markId)")
    public String WorkloadConfig = "";
    /**
     * Scheduler for group configurations
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
     * Workload tuning configurations
     */
    @Parameter(names = {"-a", "--app"}, description = "The application to be executed")
    public String application = "SimVNF";
    @Parameter(names = {"--operatorIDs"}, description = "Unique identifiers for operators")
    public String operatorIDs = "sim_vnf";
    @Parameter(names = {"--COMPUTE_COMPLEXITY"}, description = "COMPUTE_COMPLEXITY per event")
    public int COMPUTE_COMPLEXITY = 0;// 1, 10, 100
    @Parameter(names = {"--POST_COMPUTE"}, description = "POST COMPUTE_COMPLEXITY per event")
    public int POST_COMPUTE = 0;// 1, 10, 100
    @Parameter(names = {"--NUM_ACCESS"}, description = "Number of state access per transaction")
    public int NUM_ACCESS = 5;//
    @Parameter(names = {"--ratio_of_read"}, description = "ratio_of_read")
    public double ratio_of_read = 0.0; //<=1
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
    @Parameter(names = {"--complexity"}, description = "Dummy UDF complexity for state access process.")
    public Integer complexity = 0;
    /**
     * Dynamic workload
     */
    @Parameter(names = {"--shiftRate"}, description = "control the rate of the workload shift")
    public int shiftRate = 1;
    @Parameter(names = {"--phaseNum"}, description = "How many phases")
    public int phaseNum = 1;



    /**
     * Fault tolerance configurations
     */
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



    /**
     * Database configurations
     */
    @Parameter(names = {"--numItems"}, description = "NUM_ITEMS in DB.")
    public int numItems = 5000;//number of records in each table
    @Parameter(names = {"--loadDBThreadNum"}, description = "NUM_PARTITIONS in DB.")
    public int loadDBThreadNum = 4;//number of partitions in each table
    @Parameter(names = {"--tableNames"}, description = "String of table names, split by ,")
    public String tableNames = "testTable";
    @Parameter(names = {"--numberItemsForTables"}, description = "number of items for each table, split by ,")
    public String numberItemsForTables = "5000";
    @Parameter(names = {"--keyDataTypesForTables"}, description = "key data types for each table, split by ,")
    public String keyDataTypesForTables = "string";
    @Parameter(names = {"--valueDataTypesForTables"}, description = "value data types for each table, split by ,")
    public String valueDataTypesForTables = "int";
    @Parameter(names = {"--valueNamesForTables"}, description = "value names for each table, split by ,")
    public String valueNamesForTables = "vnfValue";



    /**
     * Input configurations
     */
    @Parameter(names = {"--rootFilePath"}, description = "Root path for data files.")
    public String rootPath = System.getProperty("user.home") + OsUtils.OS_wrapper("data");
    @Parameter(names = {"--inputFileType"}, description = "input file type, [txt, csv, json]")
    public int inputFileType = 0;
    @Parameter(names = {"--inputFileName"}, description = "input file name")
    public String inputFileName = "events.txt";
    @Parameter(names = {"--dataDirectory"}, description = "input file name")
    public String dataDirectory = "morphStream/data/jobs";

    @Parameter(names = {"--workloadType"}, description = "which type of dynamic workload")
    public String workloadType = "default," +
            "Up_skew,Up_skew,Up_skew,Up_abort,Up_abort,Up_abort,Down_abort,Down_abort,Down_abort,Down_skew," +
            "unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging," +
            "Down_skew,Down_skew,Up_skew,Up_skew,Up_skew,Up_abort,Up_abort,Up_abort,Down_abort,Down_abort," +
            "unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging," +
            "unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging," +
            "unchanging,unchanging,unchangin,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging";
    //OP_BFS -> OP_NS -> OP_NS_A -> OP_NS -> OP_BFS -> OP_NS -> OP_NS_A -> OP_NS
    @Parameter(names = {"--eventTypes"}, description = "String of event types, split by ,")
    public String eventTypes = "transfer;deposit";
    @Parameter(names = {"--tableNameForEvents"}, description = "table names for each type of event, split by ;")
    public String tableNameForEvents = "accounts,bookEntries;accounts,bookEntries";
    @Parameter(names = {"--keyNumberForEvents"}, description = "number of keys for each type of event, split by ;")
    public String keyNumberForEvents = "2,2;1,1"; //transfer: {account:2(src,dest). bookEntries:2(src,dest)}; deposit: {account:1(src), bookEntries:1(src)}
    @Parameter(names = {"--valueNameForEvents"}, description = "value names for each type of event, split by ;")
    public String valueNameForEvents = "transferAmount,transferAmount;depositAmount,depositAmount";
    @Parameter(names = {"--eventRatio"}, description = "event ratio for each type of event, split by ,")
    public String eventRatio = "50,50";
    @Parameter(names = {"--ratioOfMultiPartitionTransactionsForEvents"}, description = "ratio of multi partition transactions for each type of event, split by ,")
    public String ratioOfMultiPartitionTransactionsForEvents = "0.5,0.5";
    @Parameter(names = {"--stateAccessSkewnessForEvents"}, description = "state access skewness for each types of event, split by ,")
    public String stateAccessSkewnessForEvents = "0,0";
    @Parameter(names = {"--abortRatioForEvents"}, description = "abort ratio for each types of event, split by ,")
    public String abortRatioForEvents = "0,0";

    /**
     * Benchmarking and evaluation parameters
     */
    @Parameter(names = {"--measure"}, description = "enable measurement")
    public boolean enable_measurement = false;
    @Parameter(names = {"-mp"}, description = "Metric path", required = false)
    public String metric_path = rootPath + OsUtils.OS_wrapper("metric_output");
    @Parameter(names = {"--machine"}, description = "which machine to use? 0 (default): a simple one-socket machine with four cores. Add your machine specification accordingly and select them by change this specification")
    public int machine = 0;
    @Parameter(names = {"-r", "--runtime"}, description = "Runtime in seconds for the Brisk.topology (local mode only)")
    public int runtimeInSeconds = 30;
    @Parameter(names = {"--verbose"}, description = "whether print execution detail")
    public boolean verbose = false;
    @Parameter(names = {"--multicoreEvaluation"}, description = "Evaluation the multicoreScalability. Default to be false.")
    public Integer multicoreEvaluation = 0;
    @Parameter(names = {"--maxThreads"}, description = "Evaluation the multicoreScalability. The max number of threads.")
    public Integer maxThreads = 24;
    //SystemOverhead
    @Parameter(names = {"--cleanUp"}, description = "Whether to clean temporal objects. Default to be false.")
    public Integer cleanUp = 0;



    /**
     * Client configurations
     */
    @Parameter(names = {"--clientClassName"}, description = "Client class name, used for UDF Reflection")
    public String clientClassName = "cli.FastSLClient";


    /**
     * TransNFV Specific configurations
     */
    @Parameter(names = {"--numPackets"}, description = "Total number of events to process.")
    public int numPackets = 400000;
    @Parameter(names = {"--numTPGThreads"}, description = "total execution threads")
    public int numTPGThreads = 4;// default total execution threads
    @Parameter(names = {"--puncInterval"}, description = "checkpoint interval (#tuples)")
    public int puncInterval = 1000;//checkpoint per thread.
    @Parameter(names = {"--nfvExperimentPath"}, description = "The simulated input data path")
    public String nfvExperimentPath = "/home/zhonghao/IdeaProjects/transNFV/morphStream/scripts/TransNFV";
    @Parameter(names = {"--numInstances"}, description = "Number of socket listener to handle VNF instances, each for one VNF socket")
    public int numInstances = 4;
    @Parameter(names = {"--numOffloadThreads"}, description = "Number of threads in Offloading CC's executor service thread pool")
    public int numOffloadThreads = 4;
    @Parameter(names = {"--ccStrategy"}, description = "Chosen CC strategy") // 0: Partition, 1: Cache, 2: Offload, 3: TPG, 4: OpenNF, 5: CHC, 6: Adaptive
//    public String ccStrategy = "Partitioning";
    public String ccStrategy = "Replication";
//    public String ccStrategy = "Offloading";
//    public String ccStrategy = "Proactive";
//    public String ccStrategy = "Adaptive";
    @Parameter(names = {"--enableTimeBreakdown"}, description = "Enable measurement for execution time breakdown analysis or not")
    public int enableTimeBreakdown = 0;
    @Parameter(names = {"--instancePatternPunctuation"}, description = "For hardcoded instance-level punctuation control & cc switch")
    public int instancePatternPunctuation = 25000;
    @Parameter(names = {"--managerPatternPunctuation"}, description = "For manager-level punctuation control & cc switch")
    public int managerPatternPunctuation = 10000;
    @Parameter(names = {"--expID"}, description = "The running experiment ID")
    public String expID = "5.4.3";
    @Parameter(names = {"--vnfID"}, description = "The running experiment ID")
    public String vnfID = "11";
    @Parameter(names = {"--enableMemoryFootprint"}, description = "Measure runtime memory footprint or not")
    public int enableMemoryFootprint = 0;
    @Parameter(names = {"--doMVCC"}, description = "0 - SVCC, 1 - MVCC")
    public int doMVCC = 0;
//    public int doMVCC = 1;
    @Parameter(names = {"--udfComplexity"}, description = "Simulated UDF complexity in microseconds")
    public int udfComplexity = 0;
    @Parameter(names = {"--memoryIntervalMS"}, description = "Time interval to perform memory footprint measurement")
    public int memoryIntervalMS = 10;

    /** Parameters controls which csv file to read */
    @Parameter(names = {"--keySkew"})
    public int keySkew = 0;
    @Parameter(names = {"--workloadSkew"})
    public int workloadSkew = 0;
    @Parameter(names = {"--readRatio"})
    public int readRatio = 50;
    @Parameter(names = {"--locality"})
    public int locality = 0;
    @Parameter(names = {"--scopeRatio"}, description = "Ratio of per-flow requests")
    public int scopeRatio = 0;


    // Input path = "expID / workloadConfig", agreed by Generator scripts and TransNFV input
    // 5.4 workloadConfig = "vnfID / numPackets / numInstances / numItems / keySkew / workloadSkew / readRatio / locality / scopeRatio"

    // Output path = "expID / workloadConfig / systemConfig", agreed by both TransNFV output and Plot scripts
    // System config = "ccStrategy / doMVCC / udfComplexity / puncInterval / numTPGThread / numOffloadThread"

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
        /* Client configurations */
        config.put("clientClassName", clientClassName);

        /* System configurations */
        config.put("CCOption", CCOption);
        config.put("generator", generator);
        config.put("linked", linked);
        config.put("shared", shared);
        if (CCOption == 4)//S-Store enabled.
            config.put("partition", true);
        else
            config.put("partition", enable_partition);
        if (batch != -1) {
            config.put("batch", batch);
        }
        config.put("tthread", numTPGThreads);
        config.put("checkpoint", puncInterval);
        config.put("fanoutDist", fanoutDist);
        config.put("idGenType", idGenType);
        if (isCyclic == 1) {
            config.put("isCyclic", true);
        } else {
            config.put("isCyclic", false);
        }
        config.put("ratio_of_multi_partition", ratio_of_multi_partition);
        config.put("number_partitions", number_partitions);
        config.put("theta", theta);
        config.put("size_tuple", size_tuple);
        config.put("queue_size", queue_size);
        config.put("useNativeLib", useNativeLib);

        /* Dynamic switch scheduler */
        if (isDynamic == 1) {
            config.put("isDynamic", true);
            //config.put("totalEvents", phaseNum * numTPGThreads * checkpoint_interval);
            config.put("schedulersPool", schedulerPools);
            config.put("defaultScheduler", defaultScheduler);
            config.put("scheduler", defaultScheduler);
            config.put("isRuntime", isRuntime);
            config.put("bottomLine", bottomLine);
            config.put("WorkloadConfig", WorkloadConfig);
        } else {
            config.put("isDynamic", false);
            config.put("scheduler", scheduler);
            config.put("defaultScheduler", defaultScheduler);
        }
        // Group scheduler
        if (isGroup == 1) {
            config.put("isGroup", true);
            config.put("groupNum",groupNum);
            config.put("SchedulersForGroup",SchedulersForGroup);
            config.put("totalEvents",phaseNum * numTPGThreads * puncInterval);
            config.put("skewGroup",skewGroup);
            config.put("Ratio_of_Transaction_Aborts_Highest",Ratio_of_Transaction_Aborts_Highest);
        } else {
            config.put("isGroup", false);
            config.put("groupNum",1);
        }

        /* Workload configurations */
        config.put("application", application);
        config.put("operatorIDs", operatorIDs);
        config.put("ratio_of_read", ratio_of_read);
        config.put("Ratio_Of_Deposit", Ratio_Of_Deposit);
        config.put("Ratio_Of_Buying", Ratio_Of_Buying);
        config.put("State_Access_Skewness", State_Access_Skewness);
        config.put("Ratio_of_Multiple_State_Access", Ratio_of_Multiple_State_Access);
        config.put("Ratio_of_Overlapped_Keys", Ratio_of_Overlapped_Keys);
        config.put("Ratio_of_Transaction_Aborts", Ratio_of_Transaction_Aborts);
        config.put("COMPUTE_COMPLEXITY", COMPUTE_COMPLEXITY);
        config.put("POST_COMPUTE", POST_COMPUTE);
        config.put("NUM_ACCESS", NUM_ACCESS);
        config.put("Period_of_Window_Reads", Period_of_Window_Reads);
        config.put("Ratio_of_Non_Deterministic_State_Access", Ratio_of_Non_Deterministic_State_Access);
        config.put("complexity", complexity);
        config.put("windowSize", windowSize);
        // Dynamic workload configurations
        config.put("shiftRate", shiftRate);
        config.put("phaseNum", phaseNum);


        /* Fault tolerance configurations */
        config.put("FTOption", FTOption);
        config.put("parallelNum", numTPGThreads);
        config.put("compressionAlg", compressionAlg);
        config.put("snapshotInterval", snapshotInterval);
        config.put("arrivalRate", arrivalRate);
        config.put("failureTime", failureTime);
        config.put("measureInterval", measureInterval);
        config.put("maxItr",maxItr);
        // Fault tolerance relax
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

        /* Database configurations */
        config.put("NUM_ITEMS", numItems);
        config.put("loadDBThreadNum", loadDBThreadNum);
        config.put("tableNames", tableNames);
        String[] tableNameString = tableNames.split(",");
        for (int i = 0; i < tableNameString.length; i ++) {
            config.put(tableNameString[i] + "_num_items", Integer.parseInt(numberItemsForTables.split(",")[i]));
            config.put(tableNameString[i] + "_key_data_types", keyDataTypesForTables.split(",")[i]);
            config.put(tableNameString[i] + "_value_data_types", valueDataTypesForTables.split(",")[i]);
            config.put(tableNameString[i] + "_value_names", valueNamesForTables.split(",")[i]);
        }

        /* Input configurations */
        config.put("rootPath", rootPath);
        config.put("inputFileType", inputFileType);
        config.put("inputFilePath", rootPath + OsUtils.OS_wrapper("inputs/sl/events.txt"));
        config.put("inputFileName", inputFileName);
        config.put("dataDirectory", dataDirectory);
        config.put("totalEvents", numPackets);
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
            config.put(eventTypeString[i] + "_abort_ratio", abortRatioForEvents.split(",")[i]);
        }

        /* Benchmarking and evaluation configurations */
        config.put("measure", enable_measurement);
        config.put("machine", machine);
        config.put("metrics.output", metric_path);
        config.put("verbose", verbose);
        config.put("runtimeInSeconds", runtimeInSeconds);
        if (multicoreEvaluation == 0) {
            config.put("multicoreEvaluation",false);
        } else {
            config.put("multicoreEvaluation",true);
        }
        config.put("maxThreads",maxThreads);
        if (cleanUp == 0) {
            config.put("cleanUp",false);
        } else {
            config.put("cleanUp",true);
        }

        /* TransNFV workload configurations */
        config.put("nfvExperimentPath", nfvExperimentPath);
        config.put("numInstances", numInstances);
        config.put("numOffloadThreads", numOffloadThreads);
        config.put("ccStrategy", ccStrategy);
        config.put("enableTimeBreakdown", enableTimeBreakdown);
        config.put("instancePatternPunctuation", instancePatternPunctuation);
        config.put("managerPatternPunctuation", managerPatternPunctuation);
        config.put("experimentID", expID);
        config.put("vnfID", vnfID);
        config.put("enableMemoryFootprint", enableMemoryFootprint);
        config.put("doMVCC", doMVCC);
        config.put("udfComplexity", udfComplexity);
        config.put("memoryIntervalMS", memoryIntervalMS);
        config.put("keySkew", keySkew);
        config.put("workloadSkew", workloadSkew);
        config.put("readRatio", readRatio);
        config.put("locality", locality);
        config.put("scopeRatio", scopeRatio);
        String inputWorkloadPath = String.format(nfvExperimentPath + "/workload/%s/vnfID=%s/numPackets=%d/numInstances=%d/numItems=%d/keySkew=%d/workloadSkew=%d/readRatio=%d/locality=%s/scopeRatio=%d",
                expID, vnfID, numPackets, numInstances, numItems, keySkew, workloadSkew, readRatio, locality, scopeRatio);
        config.put("inputWorkloadPath", inputWorkloadPath);
        configSystem(config);
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
        //    Metrics.H2_SIZE = Metrics.NUM_ITEMS / conf.getInt("numTPGThreads");
        //}
    }
}
