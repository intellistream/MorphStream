package application.topology;


import application.bolts.comm.StringParserBolt;
import application.bolts.lr.*;
import application.datatype.util.LRTopologyControl;
import application.constants.LinearRoadConstants;
import application.constants.LinearRoadConstants.Conf;
import application.constants.LinearRoadConstants.Field;
import application.util.Configuration;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.ShuffleGrouping;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static application.constants.LinearRoadConstants.PREFIX;

/**
 * @author mayconbordin
 */
public class LinearRoad extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(LinearRoad.class);
    private final int accidentBoltThreads;
    private final int dailyExpBoltThreads;
    private final int toll_cv_BoltThreads, toll_las_BoltThreads, toll_pos_BoltThreads;


    private final int DispatcherBoltThreads;
    private final int COUNT_VEHICLES_Threads;
    private final int AccidentNotificationBoltThreads;
    private final int AccountBalanceBoltThreads;
    private final int averageSpeedThreads;
    private final int latestAverageVelocityThreads;

    public LinearRoad(String topologyName, Configuration config) {
        super(topologyName, config);
//        initilize_parser();
        DispatcherBoltThreads = config.getInt(Conf.DispatcherBoltThreads, 1);
        COUNT_VEHICLES_Threads = config.getInt(Conf.COUNT_VEHICLES_Threads, 1);
        averageSpeedThreads = config.getInt(Conf.AverageSpeedThreads, 1);
        latestAverageVelocityThreads = config.getInt(Conf.LatestAverageVelocityThreads, 1);
        toll_cv_BoltThreads = config.getInt(Conf.toll_cv_BoltThreads, 1);
        toll_las_BoltThreads = config.getInt(Conf.toll_las_BoltThreads, 1);
        toll_pos_BoltThreads = config.getInt(Conf.toll_pos_BoltThreads, 1);
        accidentBoltThreads = config.getInt(Conf.AccidentDetectionBoltThreads, 1);
        AccidentNotificationBoltThreads = config.getInt(Conf.AccidentNotificationBoltThreads, 1);
        AccountBalanceBoltThreads = config.getInt(Conf.AccountBalanceBoltThreads, 1);
        dailyExpBoltThreads = config.getInt(Conf.dailyExpBoltThreads, 1);
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
        // builder.setSpout("inputEventInjector", new InputEventInjectorSpout(), 1);//For the moment we keep just one input injector spout

        List<String> fields = new LinkedList<>(Arrays.asList(LRTopologyControl.XWAY_FIELD_NAME,
                LRTopologyControl.DIRECTION_FIELD_NAME));

        try {
            spout.setFields(new Fields(Field.TEXT));//output of a spouts
            builder.setSpout(LRTopologyControl.SPOUT, spout, spoutThreads);

            builder.setBolt(LinearRoadConstants.Component.PARSER, new StringParserBolt(parser,
                            new Fields(Field.TEXT))
                    , config.getInt(Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(LRTopologyControl.SPOUT));
//
            builder.setBolt(LRTopologyControl.DISPATCHER, new DispatcherBolt(), DispatcherBoltThreads,
                    new ShuffleGrouping(LinearRoadConstants.Component.PARSER));

            builder.setBolt(LRTopologyControl.AVERAGE_SPEED_BOLT, new AverageVehicleSpeedBolt(), averageSpeedThreads,
                    new ShuffleGrouping(
                            LRTopologyControl.DISPATCHER,
                            LRTopologyControl.POSITION_REPORTS_STREAM_ID
                    )
            );
//
            builder.setBolt(LRTopologyControl.ACCIDENT_DETECTION_BOLT, new AccidentDetectionBolt(), accidentBoltThreads,
                    new ShuffleGrouping(
                            LRTopologyControl.DISPATCHER,
                            LRTopologyControl.POSITION_REPORTS_STREAM_ID
//							, new Fields(LRTopologyControl.XWAY_FIELD_NAME, LRTopologyControl.DIRECTION_FIELD_NAME)
                    )
            );

            builder.setBolt(LRTopologyControl.COUNT_VEHICLES_BOLT, new CountVehiclesBolt(), COUNT_VEHICLES_Threads,
//					new TimestampMerger(new CountVehiclesBolt(), PositionReport.MIN_IDX), COUNT_VEHICLES_Threads,
                    new ShuffleGrouping(
                            LRTopologyControl.DISPATCHER, LRTopologyControl.POSITION_REPORTS_STREAM_ID
//							, SegmentIdentifier.getSchema()
                    )
            );


            builder.setBolt(LRTopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, new LatestAverageVelocityBolt(), latestAverageVelocityThreads,
                    new ShuffleGrouping(
                            LRTopologyControl.AVERAGE_SPEED_BOLT,
                            LRTopologyControl.LAST_AVERAGE_SPEED_STREAM_ID
                    ));

            builder.setBolt(LRTopologyControl.ACCIDENT_NOTIFICATION_BOLT_NAME, new AccidentNotificationBolt(), AccidentNotificationBoltThreads,
                    new ShuffleGrouping(LRTopologyControl.DISPATCHER, //FieldsGrouping
                            LRTopologyControl.POSITION_REPORTS_STREAM_ID// streamId
                    )
            );

            builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_POS_BOLT_NAME, new TollNotificationBolt_pos(), toll_pos_BoltThreads
                    , new ShuffleGrouping(LRTopologyControl.DISPATCHER, LRTopologyControl.POSITION_REPORTS_STREAM_ID
                    )
            );

            builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_CV_BOLT_NAME, new TollNotificationBolt_cv(), toll_cv_BoltThreads
                    , new ShuffleGrouping(LRTopologyControl.COUNT_VEHICLES_BOLT, LRTopologyControl.CAR_COUNTS_STREAM_ID)
            );

            builder.setBolt(LRTopologyControl.TOLL_NOTIFICATION_LAS_BOLT_NAME, new TollNotificationBolt_las(), toll_las_BoltThreads
                    , new ShuffleGrouping(LRTopologyControl.LAST_AVERAGE_SPEED_BOLT_NAME, LRTopologyControl.LAVS_STREAM_ID)
            );

            builder.setSink(LRTopologyControl.SINK, sink, sinkThreads
                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_POS_BOLT_NAME,
                            LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_CV_BOLT_NAME,
                            LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
                    , new ShuffleGrouping(LRTopologyControl.TOLL_NOTIFICATION_LAS_BOLT_NAME,
                            LRTopologyControl.TOLL_NOTIFICATIONS_STREAM_ID)
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
