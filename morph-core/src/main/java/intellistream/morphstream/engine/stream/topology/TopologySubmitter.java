package intellistream.morphstream.engine.stream.topology;

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

    /**
     * TODO: support different configurations in TM.
     */
    public Topology submitTopology(Topology topology, Configuration conf) throws UnhandledCaseException {
        //compile
        ExecutionGraph g = new TopologyComiler().generateEG(topology, conf);
        Collection<TopologyComponent> topologyComponents = g.topology.getRecords().values();
        if (CONTROL.enable_shared_state) {
            Metrics.COMPUTE_COMPLEXITY = conf.getInt("COMPUTE_COMPLEXITY");
            Metrics.POST_COMPUTE_COMPLEXITY = conf.getInt("POST_COMPUTE");
            Metrics.NUM_ACCESSES = conf.getInt("NUM_ACCESS");
            Metrics.NUM_ITEMS = conf.getInt("NUM_ITEMS");
            Metrics.H2_SIZE = Metrics.NUM_ITEMS / conf.getInt("tthread");
        }


        // Validity check of the config
        assert conf.getInt("totalEvents") / (conf.getInt("checkpoint") * conf.getInt("tthread")) >= 1
                && conf.getInt("totalEvents") % (conf.getInt("checkpoint") * conf.getInt("tthread")) == 0;

        // initialize AppConfig
        AppConfig.complexity = conf.getInt("complexity", 100000);
        AppConfig.windowSize = conf.getInt("windowSize", 1024);
        AppConfig.isCyclic = conf.getBoolean("isCyclic", true);
        //launch
        OM = new OptimizationManager(g, conf);//support different kinds of optimization module.
        if (CONTROL.enable_shared_state) {
            if (CONTROL.enable_log) LOG.info("DB initialize starts @" + DateTime.now());
            long start = System.nanoTime();
            int tthread = conf.getInt("tthread");
            g.topology.spinlock = new SpinLock[tthread];//number of threads -- number of cores -- number of partitions.
            g.topology.tableinitilizer = topology.txnTopology.initializeDB(g.topology.spinlock); //For simplicity, assume all table shares the same partition mapping.
            long end = System.nanoTime();
            if (CONTROL.enable_log) LOG.info("DB initialize takes:" + (end - start) / 1E6 + " ms");
            OM.lanuch(topology.db);
        } else
            OM.lanuch(topology.db);
        OM.start();
        return g.topology;
    }
}
