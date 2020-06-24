package common.topology;
import common.bolts.comm.StringParserBolt;
import common.bolts.wc.SplitSentenceBolt;
import common.bolts.wc.WordCountBolt;
import common.collections.Configuration;
import common.constants.WordCountConstants;
import common.constants.WordCountConstants.Component;
import common.constants.WordCountConstants.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.components.exception.InvalidIDException;
import sesame.components.grouping.FieldsGrouping;
import sesame.components.grouping.ShuffleGrouping;
import sesame.controller.input.scheduler.SequentialScheduler;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.topology.BasicTopology;

import static common.constants.WordCountConstants.PREFIX;
public class WordCount extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);
    public WordCount(String topologyName, Configuration config) {
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
