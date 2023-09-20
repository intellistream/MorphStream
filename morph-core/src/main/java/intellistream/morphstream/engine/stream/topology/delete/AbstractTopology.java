package intellistream.morphstream.engine.stream.topology.delete;

import intellistream.morphstream.common.constants.BaseConstants;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.operators.api.delete.AbstractSpout;
import intellistream.morphstream.engine.stream.components.operators.api.delete.BaseSink;
import intellistream.morphstream.engine.stream.topology.TopologyBuilder;
import intellistream.morphstream.util.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractTopology.class);
    public final TopologyBuilder builder;
    protected final Configuration config;
    private final String topologyName;

    protected AbstractTopology(String topologyName, Configuration config) {
        this.topologyName = topologyName;
        this.config = config;
        this.builder = new TopologyBuilder();
    }

    public String getTopologyName() {
        return topologyName;
    }

    AbstractSpout loadSpout() {
        return loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, getConfigPrefix());
    }

    protected AbstractSpout loadSpout(String name) {
        return loadSpout(BaseConstants.BaseConf.SPOUT_CLASS, String.format("%s.%s", getConfigPrefix(), name));
    }

    protected AbstractSpout loadSpout(String configKey, String configPrefix) {
        String spoutClass = config.getString(String.format(configKey, configPrefix));
        if (config.getBoolean("verbose")) {
            final String[] split = spoutClass.split("\\.");
            spoutClass = "applications.spout." + "verbose." + split[2];
            if (CONTROL.enable_log) LOG.info("spout class:" + spoutClass);
        }
        AbstractSpout spout;
        spout = (AbstractSpout) ClassLoaderUtils.newInstance(spoutClass, "spout", getLogger());
        spout.setConfigPrefix(configPrefix);
        return spout;
    }

    protected BaseSink loadSink() {
        return loadSink(BaseConstants.BaseConf.SINK_CLASS, getConfigPrefix());
    }

    protected BaseSink loadSink(String name) {
        return loadSink(BaseConstants.BaseConf.SINK_CLASS, String.format("%s.%s", getConfigPrefix(), name));
    }

    private BaseSink loadSink(String configKey, String configPrefix) {
        String sinkClass = config.getString(String.format(configKey, configPrefix));
        if (config.getBoolean("verbose")) {
            final String[] split = sinkClass.split("\\.");
            sinkClass = "applications.sink." + "verbose." + split[2];
            if (CONTROL.enable_log) LOG.info("sink class:" + sinkClass);
        }
//        if (enable_latency_measurement) {
//            final String[] split = sinkClass.split("\\.");
//            sinkClass = "applications.sink." + split[2] + "_latency";
//            if (enable_log) LOG.info("sink class:" + sinkClass);
//        }
        if (sinkClass == null)
            sinkClass = config.getString(String.format(configKey, configPrefix));
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.setConfigPrefix(configPrefix);
        return sink;
    }

    /**
     * Utility method to parse a configuration key with the application prefix and
     * component prefix.
     *
     * @param name The name of the component
     * @return The formatted configuration key
     */
    protected String getConfigKey(String name) {
        return String.format(BaseConstants.BaseConf.SINK_THREADS, String.format("%s.%s", getConfigPrefix(), name));
    }

    /**
     * Utility method to parse a configuration key with the application prefix..
     *
     * @return
     */
    protected String getConfigKey() {
        return String.format(BaseConstants.BaseConf.SPOUT_PARSER, getConfigPrefix());
    }

    public abstract void initialize();

    public abstract Topology buildTopology();

    protected abstract Logger getLogger();

    protected abstract String getConfigPrefix();
}