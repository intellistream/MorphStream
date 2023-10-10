package intellistream.morphstream.engine.stream.topology;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.TopologyComponent;
import intellistream.morphstream.engine.stream.components.exception.UnhandledCaseException;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.optimization.OptimizationManager;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.profiler.Metrics;
import intellistream.morphstream.util.AppConfig;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class TopologySubmitter {
    private final static Logger LOG = LoggerFactory.getLogger(TopologySubmitter.class);
    private OptimizationManager OM;

    public OptimizationManager getOM() {
        return OM;
    }

    public Topology submitTopology(Topology topology, Configuration conf) throws UnhandledCaseException {
        //compile
        ExecutionGraph g = new TopologyComiler().generateEG(topology, conf);

        // Validity check of the config
        assert conf.getInt("totalEvents") / (conf.getInt("checkpoint") * conf.getInt("tthread")) >= 1
                && conf.getInt("totalEvents") % (conf.getInt("checkpoint") * conf.getInt("tthread")) == 0;

        //launch
        OM = new OptimizationManager(g, conf);//support different kinds of optimization module.
        OM.lanuch(MorphStreamEnv.get().database());
        OM.start();
        return g.topology;
    }
}
