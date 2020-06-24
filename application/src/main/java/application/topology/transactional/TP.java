package application.topology.transactional;


import application.bolts.lr.*;
import application.constants.LinearRoadConstants.Conf;
import application.constants.LinearRoadConstants.Field;
import application.datatype.PositionReport;
import application.datatype.internal.AvgVehicleSpeedTuple;
import application.datatype.util.LRTopologyControl;
import application.datatype.util.SegmentIdentifier;
import application.util.Configuration;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.AllGrouping;
import sesame.components.grouping.FieldsGrouping;
import sesame.components.grouping.ShuffleGrouping;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.CONTROL.enable_force_ordering;
import static application.constants.LinearRoadConstants.Conf.Executor_Threads;
import static application.constants.TPConstants.PREFIX;
import static application.datatype.util.LRTopologyControl.*;

public class TP extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(TP.class);
//    private final int toll_cv_BoltThreads, toll_las_BoltThreads, toll_pos_BoltThreads;

    private final int DispatcherBoltThreads;
    private final int COUNT_VEHICLES_Threads;
    private final int AccidentNotificationBoltThreads;
    private final int AccountBalanceBoltThreads;
    private final int averageVehicleSpeedThreads;
    private final int averageSpeedThreads;
    private final int latestAverageVelocityThreads;
    private final int toll_BoltThreads;

    public TP(String topologyName, Configuration config) {
        super(topologyName, config);
        DispatcherBoltThreads = config.getInt(Conf.DispatcherBoltThreads, 1);
        COUNT_VEHICLES_Threads = config.getInt(Conf.COUNT_VEHICLES_Threads, 1);
        averageVehicleSpeedThreads = config.getInt(Conf.AverageVehicleSpeedThreads, 1);
        averageSpeedThreads = config.getInt(Conf.AverageSpeedThreads, 1);
        latestAverageVelocityThreads = config.getInt(Conf.LatestAverageVelocityThreads, 1);
        toll_BoltThreads = config.getInt(Conf.tollBoltThreads, 1);
        AccidentNotificationBoltThreads = config.getInt(Conf.AccidentNotificationBoltThreads, 1);
        AccountBalanceBoltThreads = config.getInt(Conf.AccountBalanceBoltThreads, 1);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {

        try {
            spout.setFields(new Fields(Field.TEXT));//output of a spouts
            builder.setSpout(LRTopologyControl.SPOUT, spout, 1);// single spout.

            //common part
//
//            builder.setBolt(LinearRoadConstants.Component.PARSER
//                    , new StringParserBolt(parser, new Fields(Field.TEXT)), config.getInt(Conf.PARSER_THREADS, 1)
//                    , new ShuffleGrouping(LRTopologyControl.SPOUT));

            builder.setBolt(LRTopologyControl.DISPATCHER,
                    new DispatcherBolt(), 1,
                    new ShuffleGrouping(LRTopologyControl.SPOUT));

            //accident query -- not in use. There's no accident.


            if (enable_force_ordering) {//force order -- improved accuracy.

                //speed query
                TimestampMerger AVS_ORDERED = new TimestampMerger(new AverageVehicleSpeedBolt(), PositionReport.TIME_IDX);
                builder.setBolt(LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME,//calculate average vehicle speed -- Avgsv, window = 1 min, slide = 1 report.
                        AVS_ORDERED,  config.getInt(Executor_Threads, 2),
//                        new AverageVehicleSpeedBolt(), averageVehicleSpeedThreads,
                        new FieldsGrouping(
                                LRTopologyControl.DISPATCHER,
                                LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                , SegmentIdentifier.getSchema()
                        )
                );


                TimestampMerger LVS_ORDERED = new TimestampMerger(new LatestAverageVelocityBolt(), AvgVehicleSpeedTuple.TIME_IDX);

                builder.setBolt(LAST_AVERAGE_SPEED_BOLT_NAME, //calculate the 5-minute average road speed considering all vehicles.
                        LVS_ORDERED, config.getInt(Executor_Threads, 2),
//                        new LatestAverageVelocityBolt(), latestAverageVelocityThreads,//window = 5 min, slide=1 report.
                        new FieldsGrouping(
                                LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME
                                , SegmentIdentifier.getSchema()
                        ));


                //count query

                builder.setBolt(COUNT_VEHICLES_BOLT, //calculate number of distinct vehicles on a road.
                        new TimestampMerger(new CountVehiclesBolt(), PositionReport.TIME_IDX),
                        config.getInt(Executor_Threads, 2),
//                        new CountVehiclesBolt(), COUNT_VEHICLES_Threads,
                        new FieldsGrouping(
                                LRTopologyControl.DISPATCHER,
                                LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                , SegmentIdentifier.getSchema()
                        )
                );


                builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
//                        new TollNotificationBolt()
                        new TimestampMerger(new TollNotificationBolt(), 1//new TollInputStreamsTsExtractor()
                        )
                        ,config.getInt(Executor_Threads, 2),

                       new FieldsGrouping(LRTopologyControl.DISPATCHER,//position report..
                                LRTopologyControl.POSITION_REPORTS_STREAM_ID,
                                new Fields(LRTopologyControl.VEHICLE_ID_FIELD_NAME))

                        , new AllGrouping(LAST_AVERAGE_SPEED_BOLT_NAME, LAVS_STREAM_ID)//broadcast latest road speed information to TN.

                        , new AllGrouping(COUNT_VEHICLES_BOLT, CAR_COUNTS_STREAM_ID)//broadcast latest vehicle count information to TN.
                );
            } else {//any order -- no accuracy guarantee.
                builder.setBolt(LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME,//calculate average vehicle speed -- Avgsv, window = 1 min, slide = 1 report.
                        new AverageVehicleSpeedBolt(), config.getInt(Executor_Threads, 2),

                        new FieldsGrouping(
                                LRTopologyControl.DISPATCHER,
                                LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                , SegmentIdentifier.getSchema()
                        )
                );

                builder.setBolt(LAST_AVERAGE_SPEED_BOLT_NAME, //calculate the 5-minute average road speed considering all vehicles.
                        new LatestAverageVelocityBolt(), config.getInt(Executor_Threads, 2),//window = 5 min, slide=1 report.
                        new FieldsGrouping(
                                LRTopologyControl.AVERAGE_VEHICLE_SPEED_FIELD_NAME
                                , SegmentIdentifier.getSchema()
                        ));


                //count query

                builder.setBolt(COUNT_VEHICLES_BOLT, //calculate number of distinct vehicles on a road.
                        new CountVehiclesBolt(), config.getInt(Executor_Threads, 2),
                        new FieldsGrouping(
                                LRTopologyControl.DISPATCHER,
                                LRTopologyControl.POSITION_REPORTS_STREAM_ID
                                , SegmentIdentifier.getSchema()
                        )
                );


                builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_BOLT_NAME,
                        new TollNotificationBolt()
//                    new TimestampMerger(new TollNotificationBolt(), new TollInputStreamsTsExtractor())
                        , toll_BoltThreads

                        , new FieldsGrouping(LRTopologyControl.DISPATCHER,//position report..
                                LRTopologyControl.POSITION_REPORTS_STREAM_ID,
                                new Fields(LRTopologyControl.VEHICLE_ID_FIELD_NAME))

                        , new AllGrouping(LAST_AVERAGE_SPEED_BOLT_NAME, LAVS_STREAM_ID)//broadcast latest road speed information to TN.

                        , new AllGrouping(COUNT_VEHICLES_BOLT, CAR_COUNTS_STREAM_ID)//broadcast latest vehicle count information to TN.
                );
            }


            builder.setSink(LRTopologyControl.SINK, sink, 1 // single sink.
                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_BOLT_NAME, TOLL_NOTIFICATIONS_STREAM_ID)
            );

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
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
