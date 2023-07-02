package common.topology.transactional;

import common.bolts.transactional.ed.cu.*;
import common.bolts.transactional.ed.es.*;
import common.bolts.transactional.ed.sc.*;
import common.bolts.transactional.ed.tc.*;
import common.bolts.transactional.ed.tcg.*;
import common.bolts.transactional.ed.tr.*;
import common.bolts.transactional.ed.wu.*;
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

import static content.Content.*;
import static utils.PartitionHelper.setPartition_interval;

public class EventDetection extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(EventDetection.class);

    public EventDetection(String topologyName, Configuration config) {
        super(topologyName, config);
    }

//    public void initialize() {
//        super.initialize();
//        sink = loadSink();
//    }

    /**
     * Load Data Later by Executors.
     *
     * @param spinlock_
     * @return TableInitilizer
     */
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
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_nocc(1)//Tweet Registrant
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_nocc(2)//Word Updater
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_nocc(3)//Trend Calculator
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.TCG, new TCGBolt_nocc(4)//Trend Calculator Gate
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
//                    builder.setBolt(EventDetectionConstants.Component.SC, new SCBolt_nocc(5)//Trend Calculator Gate
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.TCG));
//                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_nocc(6)//Cluster Updater
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.SC));
//                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_nocc(7)//Event Selector
//                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
//                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
                case CCOption_LWM: {//LWM
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_lwm(1)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_lwm(2)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_lwm(3)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.TCG, new TCGBolt_lwm(4)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
                    builder.setBolt(EventDetectionConstants.Component.SC, new SCBolt_lwm(5)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TCG));
                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_lwm(6)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SC));
                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_lwm(7)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
                case CCOption_TStream: {//T-Stream
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_ts(1)//Spout has a fid of zero.
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_ts(2)
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_ts(3)
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.TCG, new TCGBolt_ts(4)
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
                    builder.setBolt(EventDetectionConstants.Component.SC, new SCBolt_ts(5)
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TCG));
                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_ts(6)
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SC));
                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_ts(7)
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
                case CCOption_SStore: {//SStore
                    builder.setBolt(EventDetectionConstants.Component.TR, new TRBolt_sstore(1)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                    builder.setBolt(EventDetectionConstants.Component.WU, new WUBolt_sstore(2)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TR));
                    builder.setBolt(EventDetectionConstants.Component.TC, new TCBolt_sstore(3)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.WU));
                    builder.setBolt(EventDetectionConstants.Component.TCG, new TCGBolt_sstore(4)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TC));
                    builder.setBolt(EventDetectionConstants.Component.SC, new SCBolt_sstore(5)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.TCG));
                    builder.setBolt(EventDetectionConstants.Component.CU, new CUBolt_sstore(6)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.SC));
                    builder.setBolt(EventDetectionConstants.Component.ES, new ESBolt_sstore(7)//
                            , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                            , new ShuffleGrouping(EventDetectionConstants.Component.CU));
                    break;
                }
            }
            builder.setSink(EventDetectionConstants.Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(EventDetectionConstants.Component.ES)
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
        return common.constants.EventDetectionConstants.PREFIX;
    }
}
