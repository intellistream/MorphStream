package application.topology;

import application.bolts.comm.FraudDetectionParserBolt;
import application.bolts.fd.FraudPredictorBolt;
import application.constants.FraudDetectionConstants;
import application.constants.FraudDetectionConstants.Component;
import application.constants.FraudDetectionConstants.Field;
import application.util.Configuration;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.FieldsGrouping;
import sesame.components.grouping.ShuffleGrouping;
import sesame.controller.input.scheduler.SequentialScheduler;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.constants.FraudDetectionConstants.PREFIX;

public class FraudDetection extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);

    public FraudDetection(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
//        initilize_parser();
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);
//
            builder.setBolt(Component.PARSER, new FraudDetectionParserBolt(parser,
                            new Fields(Field.ENTITY_ID, Field.RECORD_DATA))
                    , config.getInt(FraudDetectionConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));
//
            builder.setBolt(Component.PREDICTOR, new FraudPredictorBolt()
                    , config.getInt(FraudDetectionConstants.Conf.PREDICTOR_THREADS, 1)
                    , new FieldsGrouping(Component.PARSER, new Fields(Field.ENTITY_ID)));

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.PREDICTOR)
            );

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
