package common.topology.transactional;

import common.bolts.transactional.ed.cu.*;
import common.bolts.transactional.ed.es.*;
import common.bolts.transactional.ed.fu.*;
import common.bolts.transactional.ed.sc.*;
import common.bolts.transactional.ed.tc.*;
import common.bolts.transactional.ed.tr.*;
import common.bolts.transactional.ed.wu.*;
import common.collections.Configuration;
import common.constants.EventDetectionConstants;
import common.constants.GrepSumConstants;

import common.constants.LinearRoadConstants.Field;
import common.datatype.util.LRTopologyControl;
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
import static common.constants.LinearRoadConstants.Conf.Executor_Threads;
import static common.constants.TPConstants.Component.EXECUTOR;
import static common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static common.constants.TPConstants.PREFIX;
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

    @Override
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        setPartition_interval((int) (Math.ceil(NUM_SEGMENTS / (double) tthread)), tthread);
        TableInitilizer ini = new EDInitializer(db, theta, tthread, config);
        ini.creates_Table(config);
        if (config.getBoolean("partition", false)) {
            for (int i = 0; i < tthread; i++)
                spinlock_[i] = new SpinLock();
            PartitionedOrderLock.getInstance().initilize(tthread);
        }
        return ini;
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));//output of a spouts
            builder.setSpout(LRTopologyControl.SPOUT, spout, spoutThreads);
            if (enable_app_combo) {
                //spout only. enable_app_combo should always be false since we are not using combo in Event Detection
            } else {
                switch (config.getInt("CCOption", 0)) {
                    case CCOption_LOCK: {//no-order
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TRBolt_nocc(0)//not available
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new WUBolt_nocc(0)//not available
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new FUBolt_nocc(0)//not available
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TCBolt_nocc(0)//not available
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new SCBolt_nocc(0)//not available
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new CUBolt_nocc(0)//not available
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new ESBolt_nocc(0)//not available
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        break;
                    }
                    case CCOption_OrderLOCK: {//LOB
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TRBolt_olb(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new WUBolt_olb(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new FUBolt_olb(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TCBolt_olb(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new SCBolt_olb(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new CUBolt_olb(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new ESBolt_olb(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        break;
                    }
                    case CCOption_LWM: {//LWM
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TRBolt_lwm(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new WUBolt_lwm(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new FUBolt_lwm(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TCBolt_lwm(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new SCBolt_lwm(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new CUBolt_lwm(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new ESBolt_lwm(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        break;
                    }
                    case CCOption_TStream: {//T-Stream
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TRBolt_ts(0)//
                                , config.getInt(GrepSumConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new WUBolt_ts(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new FUBolt_ts(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TCBolt_ts(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new SCBolt_ts(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new CUBolt_ts(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new ESBolt_ts(0)//
                                , config.getInt(EventDetectionConstants.Conf.Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        break;
                    }
                    case CCOption_SStore: {//SStore
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TRBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new WUBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new FUBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new TCBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new SCBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new CUBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        builder.setBolt(EventDetectionConstants.Component.EXECUTOR, new ESBolt_sstore(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(EventDetectionConstants.Component.SPOUT));
                        break;
                    }
                }
                builder.setSink(EventDetectionConstants.Component.SINK, sink, sinkThreads
                        , new ShuffleGrouping(EXECUTOR)
                );
            }
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
