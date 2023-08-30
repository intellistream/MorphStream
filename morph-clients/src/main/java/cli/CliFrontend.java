//package cli;
//
//import com.beust.jcommander.JCommander;
//import com.beust.jcommander.ParameterException;
//import intellistream.AppDriver;
//import intellistream.morphstream.common.constants.BaseConstants;
//import intellistream.morphstream.common.constants.GrepSumConstants;
//import intellistream.morphstream.common.constants.NonGrepSumConstants;
//import intellistream.morphstream.common.constants.SHJConstants;
//import intellistream.morphstream.common.io.Enums.platform.HP_Machine;
//import intellistream.morphstream.common.io.Enums.platform.HUAWEI_Machine;
//import intellistream.morphstream.configuration.Configuration;
//import intellistream.morphstream.configuration.Constants;
//import intellistream.morphstream.engine.stream.components.Topology;
//import intellistream.morphstream.engine.txn.durability.struct.FaultToleranceRelax;
//import intellistream.morphstream.engine.txn.lock.SpinLock;
//import intellistream.morphstream.engine.txn.profiler.MeasureTools;
//import intellistream.morphstream.engine.txn.scheduler.struct.OperationChainCommon;
//
//import static intellistream.morphstream.common.constants.LinearRoadConstants.Conf.Executor_Threads;
//import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Conf.OB_THREADS;
//import static intellistream.morphstream.common.constants.StreamLedgerConstants.Conf.SL_THREADS;
//import static intellistream.morphstream.configuration.CONTROL.*;
//import static intellistream.morphstream.engine.txn.content.LVTStreamContent.LVTSTREAM_CONTENT;
//import static intellistream.morphstream.engine.txn.content.LWMContentImpl.LWM_CONTENT;
//import static intellistream.morphstream.engine.txn.content.LockContentImpl.LOCK_CONTENT;
//import static intellistream.morphstream.engine.txn.content.SStoreContentImpl.SSTORE_CONTENT;
//import static intellistream.morphstream.engine.txn.content.TStreamContent.T_STREAMCONTENT;
//import static intellistream.morphstream.engine.txn.content.common.ContentCommon.content_type;
//import static intellistream.morphstream.engine.txn.content.common.ContentCommon.loggingRecord_type;
//import static intellistream.morphstream.engine.txn.profiler.MeasureTools.METRICS_REPORT;
//import static intellistream.morphstream.util.FaultToleranceConstants.*;
//import static intellistream.morphstream.util.FaultToleranceConstants.LOGOption_command;
//
///**
// * TODO: Implementation of a simple command line frontend for executing programs.
// */
//public class CliFrontend {
////    public static void main(String[] args) {
////        System.out.println("Hello World!");
////    }
//    protected final AppDriver driver;
//
//    public CliFrontend() {
//        driver = new AppDriver();
//    }
//
//    public void run() throws InterruptedException {
//        MeasureTools.Initialize();
//        LoadConfiguration();
//
//        // Get the descriptor for the given application
//        AppDriver.AppDescriptor app = driver.getApp(application);
//        if (app == null) {
//            throw new RuntimeException("The given application name " + application + " is invalid");
//        }
//        // In case topology names is given, create one
//        if (topologyName == null) {
//            topologyName = application;
//        }
//        // Get the topology
//        Topology topology = app.getTopology(topologyName, config);
//        topology.addMachine(platform);
//        // Run the topology
//        double rt = runTopologyLocally(topology, config);
//        if (enable_profile) {
//            if (rt != -1) {//returns normally.
//                log.info("finished measurement (k events/s):\t" + rt);
//            }
//            if (enable_shared_state) {
//                SpinLock[] spinlock = final_topology.spinlock;
//                for (SpinLock lock : spinlock) {
//                    if (lock != null)
//                        log.info("Partition" + lock + " being locked:\t" + lock.count + "\t times");
//                }
//                METRICS_REPORT(config.getInt("CCOption", 0), config.getInt("FTOption", 0), tthread, rt, config.getInt("phaseNum"), config.getInt("shiftRate"), config.getInt("snapshotInterval"));
//            }
//        }//end of profile.
//    }
//
//    private void LoadConfiguration() {
//        if (configStr == null) {
//            initializeCfg(config);
//            switch (config.getInt("machine")) {
//                case 0:
//                    this.platform = new HUAWEI_Machine();
//                    break;
//
//                case 1:
//                    this.platform = new HP_Machine();
//                    break;
//                default:
//                    this.platform = new HUAWEI_Machine();
//            }
//
//            if (enable_shared_state) {
//                //configure database.
//                switch (config.getInt("CCOption", 0)) {
//                    case Constants.CCOption_LOCK://lock_ratio
//                    case Constants.CCOption_OrderLOCK://Ordered lock_ratio
//                        content_type = LOCK_CONTENT;
//                        break;
//                    case Constants.CCOption_LWM://LWM
//                        content_type = LWM_CONTENT;
//                        break;
//                    case Constants.CCOption_MorphStream:
//                        if (config.getInt("FTOption") == 4) {
//                            content_type = LVTSTREAM_CONTENT;//records the multi-version of table record.
//                        } else {
//                            content_type = T_STREAMCONTENT;
//                        }
//                        break;
//                    case Constants.CCOption_SStore://SStore
//                        content_type = SSTORE_CONTENT;//records the multi-version of table record.
//                        break;
//                    default:
//                        System.exit(-1);
//                }
//                int tthread = config.getInt("tthread");
//                if (enable_app_combo) {
//                    config.put(BaseConstants.BaseConf.SPOUT_THREADS, tthread);
//                }
//            }
//
//            //set overhead_total parallelism, equally parallelism
//            switch (application) {
//                case "GrepSum": {
//                    config.put("app", 0);
//                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(GrepSumConstants.Conf.Executor_Threads, threads);
//                    break;
//                }
//                case "WindowedGrepSum": {
//                    config.put("app", 4);
//                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(GrepSumConstants.Conf.Executor_Threads, threads);
//                    break;
//                }
//                case "StreamLedger": {
//                    config.put("app", 1);
//                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(SL_THREADS, threads);
//                    break;
//                }
//                case "OnlineBiding": {
//                    config.put("app", 3);
//                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(OB_THREADS, threads);
//                    break;
//                }
//                case "TollProcessing": {
//                    config.put("app", 2);
//                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(Executor_Threads, threads);
//                    break;
//                }
//                case "SHJ": {
//                    config.put("app", 5);
//                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(SHJConstants.Conf.Executor_Threads, threads);
//                    break;
//                }
//                case "NonGrepSum": {
//                    config.put("app", 6);
//                    int threads = Math.max(1, (int) Math.floor((tthread)));
//                    config.put(NonGrepSumConstants.Conf.Executor_Threads, threads);
//                    break;
//                }
//            }
//            switch (config.getInt("FTOption", 0)) {
//                case 0:
//                case 1:
//                    loggingRecord_type = LOGOption_no;
//                    break;
//                case 2:
//                    loggingRecord_type = LOGOption_wal;
//                    break;
//                case 3:
//                    loggingRecord_type = LOGOption_path;
//                    break;
//                case 4:
//                    loggingRecord_type = LOGOption_lv;
//                    break;
//                case 5:
//                    loggingRecord_type = LOGOption_dependency;
//                    break;
//                case 6:
//                    loggingRecord_type = LOGOption_command;
//                    break;
//                default:
//                    System.exit(-1);
//            }
//
//            OperationChainCommon.cleanUp = config.getBoolean("cleanUp");
//            FaultToleranceRelax.isHistoryView = config.getBoolean("isHistoryView");
//            FaultToleranceRelax.isAbortPushDown = config.getBoolean("isAbortPushDown");
//            FaultToleranceRelax.isTaskPlacing = config.getBoolean("isTaskPlacing");
//            FaultToleranceRelax.isSelectiveLogging = config.getBoolean("isSelectiveLogging");
//        } else {
//            config.putAll(Configuration.fromStr(configStr));
//        }
//    }
//
////    public static void main(String[] args) {
////        if (enable_log) log.info("Program Starts..");
////        CliFrontend runner = new CliFrontend();
////        JCommander cmd = new JCommander(runner);
////        try {
////            cmd.parse(args);
////        } catch (ParameterException ex) {
////            if (enable_log) log.error("Argument error: " + ex.getMessage());
////            cmd.usage();
////        }
////        try {
////            runner.run();
////        } catch (InterruptedException ex) {
////            if (enable_log) log.error("Error in running topology locally", ex);
////        }
////    }
//
//    /*
//    * Application app = new Application(xxx)
//    *
//    * app.setInputSource()
//    * app.setOutputTarget()
//    *
//    * Can Events be abstracted???
//    * public class MyEventA extends TxnEvent {
//    *   //define customized txnEvent
//    * }
//    *
//    * public class MyEventB extends TxnEvent {
//    *   //define customized txnEvent
//    * }
//    *
//    * Table myTable = app.addTable("MyTable")
//    * myTable.setFieldNames(xxx)
//    * myTable.setFieldTypes(xxx)
//    *
//    * TransactionalSpout mySpout = app.addSpout(xxx)
//    * mySpout.
//    *
//    */
//
//}
