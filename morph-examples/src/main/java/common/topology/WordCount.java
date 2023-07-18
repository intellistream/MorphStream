package common.topology;

import common.bolts.comm.StringParserBolt;
import common.bolts.wc.SplitSentenceBolt;
import common.bolts.wc.WordCountBolt;
import common.collections.Configuration;
import common.constants.WordCountConstants;
import common.constants.WordCountConstants.Component;
import common.constants.WordCountConstants.Field;
import engine.stream.components.Topology;
import engine.stream.components.exception.InvalidIDException;
import engine.stream.components.grouping.FieldsGrouping;
import engine.stream.components.grouping.ShuffleGrouping;
import engine.stream.controller.input.scheduler.SequentialScheduler;
import engine.stream.execution.runtime.tuple.impl.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.stream.topology.BasicTopology;

import static common.constants.WordCountConstants.PREFIX;

public class WordCount extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public WordCount(String topologyName, Configuration config) {
        super(topologyName, config);
    }

    public void initialize() {
        super.initialize();
        sink = loadSink();
    }

    @Override
    public Topology buildTopology() {
        try {
            spout.setFields(new Fields(Field.TEXT));
            builder.setSpout(Component.SPOUT, spout, spoutThreads);
            StringParserBolt parserBolt = new StringParserBolt(parser, new Fields(Field.WORD));
            builder.setBolt(Component.PARSER, parserBolt
                    , config.getInt(WordCountConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt()
                    , config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1)
                    , new ShuffleGrouping(Component.PARSER));
            builder.setBolt(Component.COUNTER, new WordCountBolt()
                    , config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1)
                    , new FieldsGrouping(Component.SPLITTER, new Fields(Field.WORD))
            );
            builder.setSink(Component.SINK, sink, sinkThreads
                    , new ShuffleGrouping(Component.COUNTER));
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
