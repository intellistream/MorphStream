package application.topology;

import application.constants.BaseConstants;
import sesame.components.operators.api.BaseSink;
import application.helper.parser.Parser;
import application.util.ClassLoaderUtils;
import application.util.Configuration;
import sesame.components.operators.api.AbstractSpout;
import sesame.topology.AbstractTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 */
public abstract class CompositeTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(CompositeTopology.class);
    final int sinkThreads;
    BaseSink sink;
    Parser parser;

    CompositeTopology(String topologyName, Configuration config) {
        super(topologyName, config);
        boolean profile = config.getBoolean("profile");
        boolean benchmark = config.getBoolean("benchmark");
        int spoutThreads;
        int forwardThreads;
        if (!profile && !benchmark) {
            //spoutThreads = config.getInt(getConfigKey(BaseConstants.BaseConf.SPOUT_THREADS), 1);
            spoutThreads = config.getInt(BaseConstants.BaseConf.SPOUT_THREADS, 1);//now read from parameters.
            forwardThreads = config.getInt("SPOUT_THREADS", 1);//now read from parameters.
            sinkThreads = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
        } else {
            forwardThreads = 1;
            spoutThreads = 1;
            sinkThreads = 1;
        }
    }


    private void initilize_parser() {
        String parserClass = config.getString(getConfigKey(), null);
        if (parserClass != null) {

            parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
            parser.initialize(config);
        } else LOG.info("No parser is initialized");

    }

    @Override
    public void initialize() {
        config.setConfigPrefix(getConfigPrefix());
        AbstractSpout spout_wc = loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, WordCount.getPrefix());
        AbstractSpout spout_fd = loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, FraudDetection.getPrefix());
        AbstractSpout spout_sd = loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, SpikeDetection.getPrefix());
        AbstractSpout spout_lg = loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, LogProcessing.getPrefix());
        AbstractSpout spout_lr = loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, LinearRoad.getPrefix());
        initilize_parser();
    }
}
