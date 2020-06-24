package application.topology.latency;

import application.bolts.comm.StringParserBolt_latency;
import application.bolts.wc.SplitSentenceBolt_latency;
import application.bolts.wc.WordCountBolt_latency;
import application.constants.WordCountConstants;
import application.constants.WordCountConstants.Component;
import application.constants.WordCountConstants.Field;
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

import static application.constants.BaseConstants.BaseField.MSG_ID;
import static application.constants.WordCountConstants.PREFIX;

public class WordCount_latency extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount_latency.class);

    public WordCount_latency(String topologyName, Configuration config) {
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

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT, MSG_ID, Field.SYSTEMTIMESTAMP));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);


            StringParserBolt_latency parserBolt = new StringParserBolt_latency(parser, new Fields(Field.TEXT, MSG_ID, Field.SYSTEMTIMESTAMP));

            builder.setBolt(Component.PARSER, parserBolt
                    , config.getInt(WordCountConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));

            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt_latency()
                    , config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1)
                    , new ShuffleGrouping(Component.PARSER));

            builder.setBolt(Component.COUNTER, new WordCountBolt_latency()
                    , config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1)
                    , new FieldsGrouping(Component.SPLITTER, new Fields(Field.WORD))
//					, new ShuffleGrouping(Component.SPOUT)//workaround to ensure balanced scheduling.
            );

            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.COUNTER));
//                    , new ShuffleGrouping(Component.SPOUT));

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
