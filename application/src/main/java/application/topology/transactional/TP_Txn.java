package application.topology.transactional;


import application.bolts.lr.DispatcherBolt;
import application.bolts.transactional.tp.*;
import application.constants.LinearRoadConstants;
import application.constants.LinearRoadConstants.Field;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import application.topology.transactional.initializer.TPInitializer;
import application.util.Configuration;
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

import static application.CONTROL.enable_app_combo;
import static application.constants.LinearRoadConstants.Conf.Executor_Threads;
import static application.constants.TP_TxnConstants.Component.EXECUTOR;
import static application.constants.TP_TxnConstants.Constant.NUM_SEGMENTS;
import static application.constants.TP_TxnConstants.PREFIX;
import static state_engine.content.Content.*;
import static state_engine.utils.PartitionHelper.setPartition_interval;

public class TP_Txn extends TransactionTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TP_Txn.class);

    public TP_Txn(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
    }

    public static String getPrefix() {
        return PREFIX;
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

//            ini.loadDB(scale_factor, theta, getPartition_interval(), spinlock_);

            //initilize order locks.
            PartitionedOrderLock.getInstance().initilize(tthread);
        } else {
//            ini.loadDB(scale_factor, theta);
        }
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);


//        ini.loadData_Central(scale_factor, theta); // Prepared data by multiple threads.

//        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);

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
