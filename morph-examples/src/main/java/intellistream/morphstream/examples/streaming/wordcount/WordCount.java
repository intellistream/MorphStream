package intellistream.morphstream.examples.streaming.wordcount;

import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.exception.InvalidIDException;
import intellistream.morphstream.engine.stream.components.grouping.FieldsGrouping;
import intellistream.morphstream.engine.stream.components.grouping.ShuffleGrouping;
import intellistream.morphstream.engine.stream.controller.input.scheduler.SequentialScheduler;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;
import intellistream.morphstream.engine.stream.topology.BasicTopology;
import intellistream.morphstream.examples.streaming.wordcount.util.SplitSentenceBolt;
import intellistream.morphstream.examples.streaming.wordcount.util.StringParserBolt;
import intellistream.morphstream.examples.streaming.wordcount.util.WordCountBolt;
import intellistream.morphstream.examples.streaming.wordcount.util.WordCountConstants;
import intellistream.morphstream.examples.streaming.wordcount.util.WordCountConstants.Component;
import intellistream.morphstream.examples.streaming.wordcount.util.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static intellistream.morphstream.examples.streaming.wordcount.util.WordCountConstants.PREFIX;

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
