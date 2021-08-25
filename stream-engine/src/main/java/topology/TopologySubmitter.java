package topology;

import common.CONTROL;
import common.collections.Configuration;
import components.Topology;
import components.TopologyComponent;
import components.exception.UnhandledCaseException;
import execution.ExecutionGraph;
import lock.SpinLock;
import optimization.OptimizationManager;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.Metrics;

import java.util.Collection;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_shared_state;
import static profiler.Metrics.POST_COMPUTE_COMPLEXITY;

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
            POST_COMPUTE_COMPLEXITY = conf.getInt("POST_COMPUTE");
            Metrics.NUM_ACCESSES = conf.getInt("NUM_ACCESS");
            Metrics.NUM_ITEMS = conf.getInt("NUM_ITEMS");
            Metrics.H2_SIZE = Metrics.NUM_ITEMS / conf.getInt("tthread");
        }
        //launch
        OM = new OptimizationManager(g, conf);//support different kinds of optimization module.
        if (enable_shared_state) {
            if (enable_log) LOG.info("DB initialize starts @" + DateTime.now());
            long start = System.nanoTime();
            int tthread = conf.getInt("tthread");
            g.topology.spinlock = new SpinLock[tthread];//number of threads -- number of cores -- number of partitions.
            g.topology.tableinitilizer = topology.txnTopology.initializeDB(g.topology.spinlock); //For simplicity, assume all table shares the same partition mapping.
            long end = System.nanoTime();
            if (enable_log) LOG.info("DB initialize takes:" + (end - start) / 1E6 + " ms");
            OM.lanuch(topology.db);
        } else
            OM.lanuch(topology.db);
        OM.start();
        return g.topology;
    }
}
