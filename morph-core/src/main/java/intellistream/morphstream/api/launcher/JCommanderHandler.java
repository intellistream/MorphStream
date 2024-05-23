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
    @Parameter(names = {"--isCyclic"}, description = "isCyclic of generated OC.")
    public int isCyclic = 0;
    @Parameter(names = {"--theta"}, description = "theta")
    public double theta = 0.6; //0.6==medium contention
    @Parameter(names = {"--size_tuple"}, description = "size_tuple (number of elements in state)")
    public int size_tuple = 0;
    @Parameter(names = {"-queue_size"}, description = "Output queue size limit.", required = false)
    public int queue_size = 10000;



    /**
     * Scheduling configurations
     */
    @Parameter(names = {"--scheduler"},
            description = "Scheduler for MorphStream:" +
                    "OG_DFS,OG_DFS_A,OG_BFS,OG_BFS_A,OG_NS,OG_NS_A," +
                    "OP_DFS,OP_DFS_A,OP_BFS,OP_BFS_A,OP_NS,OP_NS_A," + "DScheduler,TStream")
    public String scheduler = "DScheduler";
    @Parameter(names = {"--isRuntime"}, description = "Collect runtime information")
    public boolean isRuntime = false;
    @Parameter(names = {"--isDynamic"}, description = "Dynamic Workload")
    public int isDynamic = 0;
    @Parameter(names = {"--schedulerPool"}, description = "Schedulers in the SchedulerPool")
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
    @Parameter(names = {"--operatorIDs"}, description = "Unique identifiers for operators")
    public String operatorIDs = "functionExecutor";
    @Parameter(names = {"--COMPUTE_COMPLEXITY"}, description = "COMPUTE_COMPLEXITY per event")
    public int COMPUTE_COMPLEXITY = 0;// 1, 10, 100
    @Parameter(names = {"--POST_COMPUTE"}, description = "POST COMPUTE_COMPLEXITY per event")
    public int POST_COMPUTE = 0;// 1, 10, 100
    @Parameter(names = {"--window_trigger_period"}, description = "Ratio of window events in the transaction.")
    public Integer Period_of_Window_Reads = 1024;
    @Parameter(names = {"--window_size"}, description = "Window Size for the window operations.")
    public Integer windowSize = 1024;
    @Parameter(names = {"--nondeterministic_ratio"}, description = "Ratio_of_Non_Deterministic_State_Access.(10 -> 0.01%)")
    public Integer Ratio_of_Non_Deterministic_State_Access = 0;
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
    @Parameter(names = {"--isRemoteDB"}, description = "isRemoteDB.")
    public int isRemoteDB = 0;// 0: local, 1: remote
    @Parameter(names = {"--NUM_ITEMS"}, description = "NUM_ITEMS in DB.")
    public int NUM_ITEMS = 8000;//number of records in each table
    @Parameter(names = {"--loadDBThreadNum"}, description = "NUM_PARTITIONS in DB.")
    public int loadDBThreadNum = 4;//number of partitions in each table
    @Parameter(names = {"--tableNames"}, description = "String of table names, split by ;")
    public String tableNames = "accounts,bookEntries";
    @Parameter(names = {"--numberItemsForTables"}, description = "number of items for each table, split by ;")
    public String numberItemsForTables = "8000;8000"; // 10000,10000
    @Parameter(names = {"--keyDataTypesForTables"}, description = "key data types for each table, split by ;")
    public String keyDataTypesForTables = "string;string";
    @Parameter(names = {"--valueDataTypesForTables"}, description = "value data types for each table, split by ;")
    public String valueDataTypesForTables = "double;double";
    @Parameter(names = {"--valueDataSizeForTables"}, description = "value size for each table, split by ;")
    public String valueSizeForTables = "128;256";
    @Parameter(names = {"--valueNamesForTables"}, description = "value names for each table, split by ;")
    public String valueNamesForTables = "balance;balance";



    /**
     * Input configurations
     */
    @Parameter(names = {"--config-str"}, required = false, description = "Path to the configuration file for the application")
    public String configStr; //TODO: if config string specified, load configs from the file using loadProperties()
    @Parameter(names = {"--rootFilePath"}, description = "Root path for data files.")
    public String rootPath = "/Users/curryzjj/hair-loss/MorphStream/Benchmark";
    @Parameter(names = {"--inputFileType"}, description = "input file type, [txt, csv, json]")
    public int inputFileType = 0;
    @Parameter(names = {"--inputFilePath"}, description = "relative path of input file to the root")
    public String inputFilePath;
    @Parameter(names = {"--inputFileName"}, description = "input file name")
    public String inputFileName = "events.txt";
    @Parameter(names = {"--dataDirectory"})
    public String dataDirectory = "data/jobs";
    @Parameter(names = {"--totalEvents"}, description = "Total number of events to process.")
    public int totalEvents = 10000;
    @Parameter(names = {"--workloadType"}, description = "which type of dynamic workload")
    public String workloadType = "default," +
            "Up_skew,Up_skew,Up_skew,Up_abort,Up_abort,Up_abort,Down_abort,Down_abort,Down_abort,Down_skew," +
            "unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging," +
            "Down_skew,Down_skew,Up_skew,Up_skew,Up_skew,Up_abort,Up_abort,Up_abort,Down_abort,Down_abort," +
            "unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging," +
            "unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging," +
            "unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging,unchanging";
    //OP_BFS -> OP_NS -> OP_NS_A -> OP_NS -> OP_BFS -> OP_NS -> OP_NS_A -> OP_NS
    @Parameter(names = {"--eventTypes"}, description = "String of event types, split by ;")
    public String eventTypes = "transfer;deposit";
    @Parameter(names = {"--tableNameForEvents"}, description = "table names for each type of event, split by ;")
    public String tableNameForEvents = "accounts,bookEntries;accounts,bookEntries";
    @Parameter(names = {"--keyNumberForEvents"}, description = "number of keys for each type of event, split by ;")
    public String keyNumberForEvents = "2,2;1,1"; //transfer: {account:2(src,dest). bookEntries:2(src,dest)}; deposit: {account:1(src), bookEntries:1(src)}
    @Parameter(names = {"--valueNameForEvents"}, description = "value names for each type of event, split by ;")
    public String valueNameForEvents = "transferAmount,transferAmount;depositAmount,depositAmount";
    @Parameter(names = {"--valueSizeForEvents"}, description = "value size for each event, split by ;")
    public String valueSizeForEvents = "128;256";
    @Parameter(names = {"--eventRatio"}, description = "event ratio for each type of event, split by ,")
    public String eventRatio = "50,50";
    @Parameter(names = {"--ratioOfMultiPartitionTransactionsForEvents"}, description = "ratio of multi partition transactions for each type of event, split by ,")//1000
    public String ratioOfMultiPartitionTransactionsForEvents = "50,50";
    @Parameter(names = {"--stateAccessSkewnessForEvents"}, description = "state access skewness for each types of event, split by ,")// 100
    public String stateAccessSkewnessForEvents = "0,0";
    @Parameter(names = {"--abortRatioForEvents"}, description = "abort ratio for each types of event, split by ,")//1000
    public String abortRatioForEvents = "0,0";

    //Cluster configuration
    @Parameter(names = {"--isDriver"}, description = "isDriver")
    public int isDriver = 1;//
    @Parameter(names = {"--workerId"}, description = "workerId")
    public int workerId = 0;//
    @Parameter(names = {"--workerNum"}, description = "total workerNum in the cluster")
    public int workerNum = 1;// default total node in the cluster
    @Parameter(names = {"--tthread"}, description = "total execution threads each worker")
    public int tthread = 4;// default total execution threads
    @Parameter(names = {"--clientNum"}, description = "total client threads")
    public int clientNum = 4;// default total client threads
    @Parameter(names = {"--frontendNum"}, description = "total frontendNum threads in the MorphStreamDriver")
    public int frontendNum = 4;// default total client threads
    @Parameter(names = {"--checkpoint_interval"}, description = "checkpoint interval (#tuples)")
    public int checkpoint_interval = 2500;//checkpoint per thread.

    //Network configuration
    @Parameter(names = {"--isRDMA"}, description = "whether is rdma connection")
    public int isRDMA = 0;
    @Parameter(names = {"--driverHost"}, description = "morphstream driver host")
    public String driverHost = "localhost";
    @Parameter(names = {"--driverPort"}, description = "morphstream driver port")
    public int driverPort = 5570;
    @Parameter(names = {"--workerHosts"}, description = "morphstream worker hosts")
    public String workerHosts = "localhost,localhost,localhost";
    @Parameter(names = {"--workerPorts"}, description = "morphstream worker ports")
    public String workerPorts = "5540,5550,5580";
    @Parameter(names = {"--CircularBufferCapacity"}, description = "CircularBufferCapacity")
    public int CircularBufferCapacity = 1024 * 1024 * 1024;
    @Parameter(names = {"--TableBufferCapacity"}, description = "TableBufferCapacity")
    public int TableBufferCapacity = 1024 * 1024 * 1024;
    @Parameter(names = {"--CacheBufferCapacity"}, description = "CacheBufferCapacity")
    public int CacheBufferCapacity = 1024 * 1024 * 1024;
    @Parameter(names = {"--RemoteOperationBufferCapacity"}, description = "RemoteOperationBufferCapacity")
    public int RemoteOperationBufferCapacity = 1024 * 1024 * 1024;
    @Parameter(names = {"--sendMessagePerFrontend"}, description = "sendMessagePerFrontend")
    public int sendMessagePerFrontend = 1000;
    @Parameter(names = {"--returnResultPerExecutor"}, description = "returnResultPerExecutor")
    public int returnResultPerExecutor = 1000;
    @Parameter(names = {"--totalBatch"}, description = "total send batch to process")
    public int totalBatch = 5;
    @Parameter(names = {"--shuffleType"}, description = "shuffleType: Random (0), Sort (1), Static Partition(2), Optimized (3)")
    public int shuffleType = 0;

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
    public String clientClassName = "client.SLClient";



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
        config.put("linked", linked);
        config.put("shared", shared);
        if (CCOption == 4)//S-Store enabled.
            config.put("partition", true);
        else
            config.put("partition", enable_partition);
        if (batch != -1) {
            config.put("batch", batch);
        }
        config.put("tthread", tthread);
        config.put("clientNum", clientNum);
        config.put("workerNum", workerNum);
        config.put("workerId", workerId);
        config.put("frontendNum", frontendNum);
        if (isDriver == 1) {
            config.put("isDriver", true);
        } else {
            config.put("isDriver", false);
        }
        if (isRDMA == 1) {
            config.put("isRDMA", true);
            config.put("morphstream.rdma.driverHost", driverHost);
            config.put("morphstream.rdma.driverPort", driverPort);
            config.put("morphstream.rdma.workerPorts", workerPorts);
            config.put("morphstream.rdma.workerHosts", workerHosts);
        } else {
            config.put("isRDMA", false);
            config.put("morphstream.socket.driverHost", driverHost);
            config.put("morphstream.socket.driverPort", driverPort);
            config.put("morphstream.socket.workerPorts", workerPorts);
            config.put("morphstream.socket.workerHosts", workerHosts);
        }
        config.put("CircularBufferCapacity", CircularBufferCapacity);
        config.put("TableBufferCapacity", TableBufferCapacity);
        config.put("CacheBufferCapacity", CacheBufferCapacity);
        config.put("RemoteOperationBufferCapacity", RemoteOperationBufferCapacity);
        config.put("sendMessagePerFrontend", sendMessagePerFrontend);
        config.put("maxMessageCapacity", sendMessagePerFrontend * frontendNum);
        config.put("returnResultPerExecutor", returnResultPerExecutor);
        config.put("maxResultsCapacity", returnResultPerExecutor * tthread);
        config.put("totalBatch", totalBatch);
        config.put("shuffleType", shuffleType);


        config.put("checkpoint", checkpoint_interval);
        config.put("fanoutDist", fanoutDist);
        if (isCyclic == 1) {
            config.put("isCyclic", true);
        } else {
            config.put("isCyclic", false);
        }
        config.put("theta", theta);
        config.put("size_tuple", size_tuple);
        config.put("queue_size", queue_size);

        /* Dynamic switch scheduler */
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
            config.put("defaultScheduler", defaultScheduler);
        }
        // Group scheduler
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

        /* Workload configurations */
        config.put("operatorIDs", operatorIDs);
        config.put("COMPUTE_COMPLEXITY", COMPUTE_COMPLEXITY);
        config.put("POST_COMPUTE", POST_COMPUTE);
        config.put("Period_of_Window_Reads", Period_of_Window_Reads);
        config.put("Ratio_of_Non_Deterministic_State_Access", Ratio_of_Non_Deterministic_State_Access);
        config.put("complexity", complexity);
        config.put("windowSize", windowSize);
        // Dynamic workload configurations
        config.put("shiftRate", shiftRate);
        config.put("phaseNum", phaseNum);


        /* Fault tolerance configurations */
        config.put("FTOption", FTOption);
        config.put("parallelNum", tthread);
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
        if (isRemoteDB == 0) {
            config.put("isRemoteDB", false);
        } else {
            config.put("isRemoteDB", true);
        }
        config.put("NUM_ITEMS", NUM_ITEMS);
        config.put("loadDBThreadNum", tthread);
        config.put("tableNames", tableNames);
        String[] tableNameString = tableNames.split(";");
        for (int i = 0; i < tableNameString.length; i ++) {
            config.put(tableNameString[i] + "_num_items", Integer.parseInt(numberItemsForTables.split(";")[i]));
            config.put(tableNameString[i] + "_key_data_types", keyDataTypesForTables.split(";")[i]);
            config.put(tableNameString[i] + "_value_data_types", valueDataTypesForTables.split(";")[i]);
            config.put(tableNameString[i] + "_value_size", Integer.parseInt(valueSizeForTables.split(";")[i]));
            config.put(tableNameString[i] + "_value_names", valueNamesForTables.split(";")[i]);//Field
        }

        /* Input configurations */
        config.put("rootPath", rootPath);
        config.put("inputFileType", inputFileType);
        config.put("inputFilePath", rootPath + OsUtils.OS_wrapper("inputs") + OsUtils.OS_wrapper(clientClassName) + OsUtils.OS_wrapper("event.txt"));
        config.put("dataDirectory", rootPath + OsUtils.OS_wrapper("data") + OsUtils.OS_wrapper("jobs"));
        config.put("totalEvents", totalEvents);
        config.put("workloadType", workloadType);
        config.put("eventTypes", eventTypes);
        String[] eventTypeString = eventTypes.split(";");
        for (int i = 0; i < eventTypeString.length; i ++) {
            config.put(eventTypeString[i] + "_tables", tableNameForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_key_number", keyNumberForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_values", valueNameForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_value_length", valueSizeForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_event_ratio", eventRatio.split(";")[i]);
            config.put(eventTypeString[i] + "_ratio_of_multi_partition_transactions", ratioOfMultiPartitionTransactionsForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_state_access_skewness", stateAccessSkewnessForEvents.split(";")[i]);
            config.put(eventTypeString[i] + "_abort_ratio", abortRatioForEvents.split(";")[i]);
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
        //    Metrics.H2_SIZE = Metrics.NUM_ITEMS / conf.getInt("tthread");
        //}
    }
}
