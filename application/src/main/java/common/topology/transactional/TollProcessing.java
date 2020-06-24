package common.topology.transactional;
import common.bolts.lr.DispatcherBolt;
import common.bolts.transactional.tp.*;
import common.constants.LinearRoadConstants;
import common.constants.LinearRoadConstants.Field;
import common.datatype.util.LRTopologyControl;
import common.datatype.util.SegmentIdentifier;
import common.topology.transactional.initializer.TPInitializer;
import common.collections.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.FieldsGrouping;
import sesame.components.grouping.ShuffleGrouping;
import sesame.controller.input.scheduler.SequentialScheduler;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.topology.TransactionTopology;
import state_engine.common.PartitionedOrderLock;
import state_engine.common.SpinLock;
import state_engine.transaction.TableInitilizer;

import static common.CONTROL.enable_app_combo;
import static common.constants.LinearRoadConstants.Conf.Executor_Threads;
import static common.constants.TPConstants.Component.EXECUTOR;
import static common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static common.constants.TPConstants.PREFIX;
import static state_engine.content.Content.*;
import static state_engine.utils.PartitionHelper.setPartition_interval;
public class TollProcessing extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TollProcessing.class);
    public TollProcessing(String topologyName, Configuration config) {
        super(topologyName, config);
    }
    public void initialize() {
        super.initialize();
        sink = loadSink();
    }
    //TODO: Clean this method..
    @Override
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double scale_factor = config.getDouble("scale_factor", 1);
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        setPartition_interval((int) (Math.ceil(NUM_SEGMENTS / (double) tthread)), tthread);
        TableInitilizer ini = new TPInitializer(db, scale_factor, theta, tthread, config);
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
                //spout only.
            } else {
                builder.setBolt(LRTopologyControl.DISPATCHER,
                        new DispatcherBolt(), 1,
                        new ShuffleGrouping(LRTopologyControl.SPOUT));
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