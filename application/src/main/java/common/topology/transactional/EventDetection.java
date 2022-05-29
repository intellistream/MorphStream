package common.topology.transactional;

import common.bolts.transactional.tp.*;
import common.collections.Configuration;
import common.constants.LinearRoadConstants;
import common.constants.LinearRoadConstants.Field;
import common.datatype.util.LRTopologyControl;
import common.datatype.util.SegmentIdentifier;
import common.topology.transactional.initializer.EDInitializer;
import common.topology.transactional.initializer.TPInitializer;
import components.Topology;
import components.exception.InvalidIDException;
import components.grouping.FieldsGrouping;
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

    // Is this needed???
    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    // Copied from TP - Check this again!
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

    // Copied from TP - Check this again!
    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));//output of a spouts
            builder.setSpout(LRTopologyControl.SPOUT, spout, spoutThreads);
            if (enable_app_combo) {
                //spout only.
            } else {
                switch (config.getInt("CCOption", 0)) {
                    case CCOption_LOCK: {//no-order
                        builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TPBolt_nocc(0)//not available
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(LinearRoadConstants.Component.SPOUT));
                        break;
                    }
                    case CCOption_OrderLOCK: {//LOB
                        builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TPBolt_olb(0)//
                                , config.getInt(Executor_Threads, 2),
                                new FieldsGrouping(
                                        LRTopologyControl.DISPATCHER,
                                        LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                        , SegmentIdentifier.getSchema())
                        );
                        break;
                    }
                    case CCOption_LWM: {//LWM
                        builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TPBolt_lwm(0) {
                                }//
                                , config.getInt(Executor_Threads, 2),
                                new FieldsGrouping(
                                        LRTopologyControl.DISPATCHER,
                                        LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                        , SegmentIdentifier.getSchema())
                        );
                        break;
                    }
                    case CCOption_TStream: {//T-Stream
                        builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TPBolt_ts(0)//
                                , config.getInt(Executor_Threads, 2),
                                new FieldsGrouping(
                                        LRTopologyControl.DISPATCHER,
                                        LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                        , SegmentIdentifier.getSchema())
                        );
                        break;
                    }
                    case CCOption_SStore: {//SStore
                        builder.setBolt(LinearRoadConstants.Component.EXECUTOR, new TPBolt_SSTORE(0)//
                                , config.getInt(Executor_Threads, 2)
                                , new ShuffleGrouping(LinearRoadConstants.Component.SPOUT));
                        break;
                    }
                }
                builder.setSink(LinearRoadConstants.Component.SINK, sink, sinkThreads
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
