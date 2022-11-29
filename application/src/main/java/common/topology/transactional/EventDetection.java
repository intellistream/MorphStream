package common.topology.transactional;

import common.bolts.transactional.ed.cu.*;
import common.bolts.transactional.ed.cug.CUGBolt_ts;
import common.bolts.transactional.ed.es.*;
import common.bolts.transactional.ed.tc.*;
import common.bolts.transactional.ed.tcg.TCGBolt_ts;
import common.bolts.transactional.ed.tr.*;
import common.bolts.transactional.ed.trg.TRGBolt_ts;
import common.bolts.transactional.ed.wu.*;
import common.bolts.transactional.ed.wug.WUGBolt_ts;
import common.collections.Configuration;
import common.constants.EventDetectionConstants;
import common.topology.transactional.initializer.EDInitializer;

import components.Topology;
import components.exception.InvalidIDException;
import components.grouping.ShuffleGrouping;
import controller.input.scheduler.SequentialScheduler;
import execution.runtime.tuple.impl.Fields;
import lock.PartitionedOrderLock;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topology.TransactionTopology;
import transaction.TableInitilizer;

import static common.CONTROL.enable_app_combo;
import static common.constants.EventDetectionConstants.Conf.Executor_Threads;
import static common.constants.EventDetectionConstants.PREFIX;
import static content.Content.*;
import static utils.PartitionHelper.setPartition_interval;

public class EventDetection extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(EventDetection.class);

    public EventDetection(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    //TODO: Copied from TP, check if this is needed
    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    /**
     * Load Data Later by Executors.
     *
     * @param spinlock_
     * @return TableInitilizer
     */

    //TODO: Copied from GSW, change accordingly
    @Override
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        int numberOfStates = config.getInt("NUM_ITEMS");
        setPartition_interval((int) (Math.ceil(numberOfStates / (double) tthread)), tthread);
        TableInitilizer ini = new EDInitializer(db, numberOfStates, theta, tthread, config);
        ini.creates_Table(config);
        if (config.getBoolean("partition", false)) {
            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();
            //initilize order locks.
            PartitionedOrderLock.getInstance().initilize(tthread);
        }
        return ini;
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(EventDetectionConstants.Field.TEXT));//output of a spouts
            builder.setSpout(EventDetectionConstants.Component.SPOUT, spout, spoutThreads); //TODO: Change the constant to FileSpout

            switch (config.getInt("CCOption", 0)) {
                case CCOption_LOCK: {//no-order
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_nocc(0)//Tweet Registrant
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_nocc(0)//Word Updater
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_nocc(0)//Trend Calculator
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_nocc(0)//Cluster Updater
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_nocc(0)//Event Selector
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
                case CCOption_OrderLOCK: {//LOB
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_olb(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_olb(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_olb(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_olb(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_olb(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
                case CCOption_LWM: {//LWM
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_lwm(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_lwm(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_lwm(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_lwm(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_lwm(0)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
                case CCOption_TStream: {//T-Stream
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_ts(0)
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.TRG, new TRGBolt_ts(0)
                            , config.getInt(EventDetectionConstants.Conf.Gate_Threads, 1)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
//                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_ts(0)
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.TRG));
//                    builder.setBolt(EventDetectionConstants.Component.WUG, new WUGBolt_ts(0)
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 1)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
//                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_ts(0)
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.WUG));
//                    builder.setBolt(EventDetectionConstants.Component.TCG, new TCGBolt_ts(0)
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 1)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
//                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_ts(0)
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.TCG));
//                    builder.setBolt(EventDetectionConstants.Component.CUG, new CUGBolt_ts(0)
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 1)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
//                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_ts(0)
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.CUG));
                    break;
                }
                case CCOption_SStore: {//SStore
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_sstore(0)//
                            , config.getInt(Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_sstore(0)//
                            , config.getInt(Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_sstore(0)//
                            , config.getInt(Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_sstore(0)//
                            , config.getInt(Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_sstore(0)//
                            , config.getInt(Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
            }
//            builder.setSink(EventDetectionConstants.Component.SINK, sink, sinkThreads
//                    , new ShuffleGrouping(EventDetectionConstants.Component.ES)
//            );
            builder.setSink(EventDetectionConstants.Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(EventDetectionConstants.Component.TRG)
            );

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology(db, this);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
