package sesame.optimization;
import common.platform.Platform;
import common.collections.Configuration;
import common.collections.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.execution.ExecutionGraph;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.impl.scaling.scalingOptimization;
import sesame.optimization.impl.scheduling.*;
import sesame.optimization.model.Constraints;

import java.io.FileNotFoundException;
import java.io.IOException;
/**
 * TODO: The execution should be allocated in NUMA-aware manner.
 * TODO: the current optimization goal is only the output rate, support more in future!
 * This allocator will compute an optimal allocation txn
 * <profiling>
 * Core i7 Xeon 5500 Series Data Source Latency (approximate)               [Pg. 22]
 * <profiling>
 * local  L1 CACHE hit,                              ~4 cycles (   2.1 -  1.2 ns )
 * local  L2 CACHE hit,                             ~10 cycles (   5.3 -  3.0 ns )
 * local  L3 CACHE hit, line unshared               ~40 cycles (  21.4 - 12.0 ns )
 * local  L3 CACHE hit, shared line in another core ~65 cycles (  34.8 - 19.5 ns )
 * local  L3 CACHE hit, modified in another core    ~75 cycles (  40.2 - 22.5 ns )
 * <profiling>
 * remote L3 CACHE (Ref: Fig.1 [Pg. 5])        ~100-300 cycles ( 160.7 - 30.0 ns )
 * <profiling>
 * local  DRAM                                                   ~60 ns
 * remote DRAM                                                  ~100 ns
 */
public class Optimizer {
    private final static Logger LOG = LoggerFactory.getLogger(Optimizer.class);
    final ExecutionGraph graph;
    private final int numCPUs;
    private final Configuration conf;
    private final Constraints cons;
    private final int numNodes;
    private SchedulingPlan currentPlan;
    public Optimizer(ExecutionGraph g, boolean benchmark,
                     Configuration conf, Platform p, SchedulingPlan scaling_plan) {
        this.conf = conf;
        this.currentPlan = scaling_plan;
        numNodes = conf.getInt("num_socket", 1);
        numCPUs = conf.getInt("num_cpu", 1);
        this.cons = new Constraints(numNodes, numCPUs, p);
        LOG.info("number of CPUs:" + numCPUs);
        LOG.info("number of num_socket:" + numNodes);
        LOG.info("GC factor:" + conf.getDouble("gc_factor", 1));
        this.graph = g;
    }
    public SchedulingPlan benchmark_plan(int cnt, String prefix) {
        SchedulingPlan SP = new SchedulingPlan(graph, numNodes, cons, conf, null);
        LOG.info("Start benchmarking");
        try {
            SP.buildFromFileForBenchmark(cnt, prefix);
            currentPlan = SP;
        } catch (FileNotFoundException ignored) {
        }
        currentPlan.set_success();
        return currentPlan;
    }
    public SchedulingPlan manual_plan(int cnt, String prefix) {
        SchedulingPlan SP = new SchedulingPlan(graph, numNodes, cons, conf, null);
        try {
            SP.mapFromFile(cnt, prefix);
            currentPlan = SP;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        currentPlan.set_success();
        return currentPlan;
    }
    public SchedulingPlan load_opt_plan(Topology topology) {
        SchedulingPlan SP = null;//new SchedulingPlan(topology, numNodes, cons, conf, true);
        try {
            SP = SchedulingPlan.deserializeMyself(conf, numNodes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        currentPlan = SP;
        currentPlan.set_success();
        return currentPlan;
    }
    /**
     * @return the txn plan
     */
    public SchedulingPlan optimize_plan() {
        double current_output_rate = 0;
        //  GraphMetrics gM = new GraphMetrics(graph);//clean_executorInformation cache here.
        if (currentPlan != null) {
            //build the optimization algorithm here. The algorithm should also produce a mapping file
            current_output_rate = currentPlan.getOutput_rate(true);
            LOG.info("-------Current allocation plan------------");
            currentPlan.planToString(false, true);
//            cons.show(graph, currentPlan);
        }
        //TODO: we can have a control logic here to determine if we want to re-lanuch.
        SchedulingPlan new_plan = allocation(graph);
        if (new_plan == null || !new_plan.success()) {
            LOG.info("failed to find valid new plan, use original plan instead.");
            new_plan = new SchedulingPlan(currentPlan, true);
        } else {
            new_plan.planToString(false, true);
        }
        LOG.info("measure_end the optimized plan before writing plan to disk");
        final boolean check = cons.check(new_plan);
        if (!check) {
            LOG.info("Constraint measure_end failed!!");
            LOG.info(cons.show(new_plan));
            System.exit(-1);
        } else {
            LOG.info("Constraint measure_end success.");
            LOG.info(cons.show(new_plan));
        }
        double optimize_output_rate = new_plan.getOutput_rate(false);
//		if (current_output_rate > optimize_output_rate) {
//			LOG.info("no better plan found! CPU relax is:" + cons.relax_cpu
//					+ "\tMemory relax is:" + cons.relax_memory
//					+ "\tQPI relax is:" + cons.relax_qpi);
//			LOG.info("Current predicted execution graph output rate (events/ms):" + current_output_rate * 1E6);
//			LOG.info("Predicted execution graph output rate (events/ms):" + optimize_output_rate * 1E6);
//		} else if (current_output_rate < optimize_output_rate) {
//			if (currentPlan != null) {
//				LOG.info("Better plan found! CPU relax is:" + cons.relax_cpu
//						+ "\tMemory relax is:" + cons.relax_memory
//						+ "\tQPI relax is:" + cons.relax_qpi);
//			}
//			//LOG.DEBUG("Current execution graph output rate (events/ms):" + current_output_rate * 1E6);
//			//LOG.DEBUG("-------Optimized allocation plan------------");
//			new_plan.planToFile(conf);
//			//LOG.DEBUG("Predicted execution graph output rate (events/ms):" + optimize_output_rate * 1E6);
//			currentPlan = new_plan;
//			conf.put("predict", optimize_output_rate * 1E6);
////            cons.show(graph, currentPlan);
//		} else if (currentPlan != null && cons.ResourceEfficient(new_plan, currentPlan)) {
//			//LOG.DEBUG("Both plans have same output, but new plan is more resource efficient");
//			new_plan.planToFile(conf);
//			currentPlan = new_plan;
//			conf.put("predict", optimize_output_rate * 1E6);
//		}
        //conf.put("Model Output:",optibemize_output_rate);
        new_plan.planToFile(conf, true);
        return new_plan;
    }
    /**
     * TODO: support different algorithms later.
     * TODO: implement the worst case plan in future.
     *
     * @param graph
     * @return (possible) better txn plan
     */
    private SchedulingPlan allocation(final ExecutionGraph graph) {
//		conf.put("backPressure",false);
        {//different optimization algorithm..
            if (conf.getBoolean("random", false)) {
                if (conf.getBoolean("worst", false)) {
                    return new randomSearch_Constraints(graph, numNodes, numCPUs, cons, conf)
                            .Search(conf.getBoolean("worst", false), 30000); // constraint-aware
                } else {
                    return new randomPlan_Constraints(graph, numNodes, numCPUs, cons, conf)
                            .Search(false, 30000);//constraint-no aware.
                }
            } else if (conf.getBoolean("roundrobin", false)) {
                return new roundrobin(graph, numNodes, numCPUs, cons, conf)
                        .Search(conf.getBoolean("worst", false), 30000); // constraint-aware
            } else if (conf.getBoolean("toff", false)) {
                return new TOFF(graph, numNodes, numCPUs, cons, conf)
                        .Search(false, -1);
            } else {
                if (OsUtils.isMac()) {//for test purpose
                    return new BranchAndBound(graph, numNodes, numCPUs, cons, conf, null)
                            .Search(conf.getBoolean("worst", false), 1000);
                }
                return new BranchAndBound(graph, numNodes, numCPUs, cons, conf, null)
                        .Search(conf.getBoolean("worst", false), 30000);
            }
//               return      new randomSearch_Constraints(graph,numNodes,numCPUs,cons).Search(true,50000);
            //what if we assume resource consumption and process rate is fixed...
            //new ... txn algorithm.
        }
    }
    public SchedulingPlan scalingPlan(boolean random) {
        final SchedulingPlan scaling_plan;
        if (OsUtils.isMac()) {
            scaling_plan = new scalingOptimization(graph, numNodes, numCPUs, cons, conf)
                    .Search(random, 20000);
        } else {
            scaling_plan = new scalingOptimization(graph, numNodes, numCPUs, cons, conf)
                    .Search(random, 20000);
        }
        scaling_plan.finilize();
        return scaling_plan;
    }
}
