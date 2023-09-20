package intellistream.morphstream.examples.streaming;

import intellistream.morphstream.common.connectors.MemFileSpout;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.exception.InvalidIDException;
import intellistream.morphstream.engine.stream.components.grouping.FieldsGrouping;
import intellistream.morphstream.engine.stream.components.grouping.ShuffleGrouping;
import intellistream.morphstream.engine.stream.controller.input.scheduler.SequentialScheduler;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;
import intellistream.morphstream.engine.stream.topology.delete.BasicTopology;
import intellistream.morphstream.examples.streaming.util.SplitSentenceBolt;
import intellistream.morphstream.examples.streaming.util.StringParserBolt;
import intellistream.morphstream.examples.streaming.util.WordCountBolt;
import intellistream.morphstream.examples.streaming.util.WordCountConstants;
import intellistream.morphstream.examples.streaming.util.WordCountConstants.Component;
import intellistream.morphstream.examples.streaming.util.WordCountConstants.Field;
import intellistream.morphstream.examples.utils.MeasureSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.examples.streaming.util.WordCountConstants.PREFIX;

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
            builder.setSpout(Component.SPOUT, new MemFileSpout(), spoutThreads);
            StringParserBolt parserBolt = new StringParserBolt(parser, new Fields(Field.WORD));
            builder.setBolt(Component.PARSER, parserBolt
                    , config.getInt(WordCountConstants.Conf.PARSER_THREADS, 1)
                    , new ShuffleGrouping(Component.SPOUT));
            builder.setBolt(Component.SPLITTER, new SplitSentenceBolt()
                    , config.getInt(WordCountConstants.Conf.SPLITTER_THREADS, 1)
                    , new ShuffleGrouping(Component.PARSER));
            builder.setBolt(Component.COUNTER, new WordCountBolt()
                    , config.getInt(WordCountConstants.Conf.COUNTER_THREADS, 1)
                    , new FieldsGrouping(Component.SPLITTER, new Fields(Field.WORD)));
            builder.setSink(Component.SINK, new MeasureSink(), sinkThreads
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
