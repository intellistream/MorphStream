package intellistream.morphstream.examples.tsp.tollprocessing;

import intellistream.morphstream.examples.tsp.tollprocessing.util.datatype.util.LRTopologyControl;
import intellistream.morphstream.examples.tsp.tollprocessing.util.datatype.util.SegmentIdentifier;
import intellistream.morphstream.examples.tsp.tollprocessing.op.*;
import intellistream.morphstream.examples.tsp.tollprocessing.util.TPInitializer;
import intellistream.morphstream.common.constants.LinearRoadConstants;
import intellistream.morphstream.common.constants.LinearRoadConstants.Field;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.exception.InvalidIDException;
import intellistream.morphstream.engine.stream.components.grouping.FieldsGrouping;
import intellistream.morphstream.engine.stream.components.grouping.ShuffleGrouping;
import intellistream.morphstream.engine.stream.controller.input.scheduler.SequentialScheduler;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;
import intellistream.morphstream.engine.stream.topology.delete.TransactionTopology;
import intellistream.morphstream.engine.txn.lock.PartitionedOrderLock;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.transaction.TableInitilizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.common.constants.LinearRoadConstants.Conf.Executor_Threads;
import static intellistream.morphstream.common.constants.TPConstants.Component.EXECUTOR;
import static intellistream.morphstream.common.constants.TPConstants.PREFIX;
import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.util.PartitionHelper.setPartition_interval;

public class TollProcessing extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TollProcessing.class);

    public TollProcessing(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }
    @Override
    public TableInitilizer initializeDB(SpinLock[] spinlock_) {
        double theta = config.getDouble("theta", 1);
        int tthread = config.getInt("tthread");
        int numberOfStates = config.getInt("NUM_ITEMS");
        setPartition_interval((int) (Math.ceil(numberOfStates / (double) tthread)), tthread);
        TableInitilizer ini = new TPInitializer(db, numberOfStates, theta, tthread, config);
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
                    case CCOption_MorphStream: {//MorphStream
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
        return builder.createTopology();
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
