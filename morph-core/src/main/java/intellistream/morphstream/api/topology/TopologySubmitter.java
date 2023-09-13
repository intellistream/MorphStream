package intellistream.morphstream.api.topology;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.Topology;
import intellistream.morphstream.engine.stream.components.TopologyComponent;
import intellistream.morphstream.engine.stream.components.exception.UnhandledCaseException;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.optimization.OptimizationManager;
import intellistream.morphstream.engine.stream.topology.TopologyComiler;
import intellistream.morphstream.engine.txn.lock.SpinLock;
import intellistream.morphstream.engine.txn.profiler.Metrics;
import intellistream.morphstream.util.AppConfig;
import org.joda.time.DateTime;

import java.util.Collection;

public class TopologySubmitter {

    public ExecutionGraph generateEG(Topology topology, Configuration conf) {
        //Construct Brisk.execution Graph structure based on information from this Brisk.topology.
        return new ExecutionGraph(topology, null, conf);
    }
    public Topology submitTopology() throws UnhandledCaseException {
        //compile
        ExecutionGraph g = generateEG(topology, conf);
        Collection<TopologyComponent> topologyComponents = g.topology.getRecords().values();

        //launch
        OptimizationManager OM = new OptimizationManager(g, MorphStreamEnv.get().configuration());//support different kinds of optimization module.
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
