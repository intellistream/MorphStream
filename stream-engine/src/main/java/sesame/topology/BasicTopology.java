package sesame.topology;

import application.constants.BaseConstants;
import application.helper.parser.Parser;
import application.util.ClassLoaderUtils;
import application.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.operators.api.AbstractSpout;
import sesame.components.operators.api.BaseSink;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 */
public abstract class BasicTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BasicTopology.class);
    protected final int spoutThreads;
    protected final int sinkThreads;
    protected AbstractSpout spout;
    protected BaseSink sink;
    protected Parser parser;

    protected BasicTopology(String topologyName, Configuration config) {
        super(topologyName, config);
        spoutThreads = config.getInt(BaseConstants.BaseConf.SPOUT_THREADS, 1);//now read from parameters.
        sinkThreads = config.getInt(BaseConstants.BaseConf.SINK_THREADS, 1);
    }

    protected void initilize_parser() {
        String parserClass = config.getString(getConfigKey(), null);
        if (parserClass != null) {

            parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);

            parser.initialize(config);
        } else LOG.info("No parser is initialized");

    }

    @Override
    public void initialize() {
        config.setConfigPrefix(getConfigPrefix());
        spout = loadSpout();
        initilize_parser();
    }
}
