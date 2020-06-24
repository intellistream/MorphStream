package sesame.optimization.impl.scaling;

import application.util.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.components.TopologyComponent;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.LocalPlanScheduler;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.impl.scheduling.BranchAndBound;
import sesame.optimization.impl.scheduling.TOFF_hardConstraint;
import sesame.optimization.impl.scheduling.randomPlan_NoConstraints;
import sesame.optimization.impl.scheduling.roundrobin;
import sesame.optimization.model.Constraints;
import sesame.optimization.model.cache;

import java.util.*;

import static application.constants.BaseConstants.BaseComponent.SPOUT;
import static application.util.Constants.default_sourceRate;


/**
 * Created by tony on 3/20/2017.
 */
public class scalingOptimization extends LocalPlanScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(scalingOptimization.class);
    private final Topology topology;//set_executor_ready topology
    private final double input_rate_upperbound;
    private final Configuration conf;
    //    double relax_cpu;
//    double relax_memory;
//    double relax_qpi;
    boolean backPressure;
    private int scale_upperbound = 2;//otherwise give a specified constraint.
    private long start_time;
    private long time_out = (long) (20 * 60 * 1E9);//scaling optimization allows up to 20 mins
    private boolean random;

    public scalingOptimization(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf) {
        this.topology = graph.topology;
        this.conf = conf;
        this.numNodes = numNodes;
        this.numCPUs = numCPUs;
        this.cons = cons; //this.relax_cpu = this.relax_memory = this.relax_qpi = relax;//set default relax
        scale_upperbound = numNodes * numCPUs;//80 / numNodes;//OsUtils.TotalCores(); allow 10 more threads per socket to oversubscribe.？？
        LOG.info("Scale upper bound:" + scale_upperbound);
        if (conf.getInt("machine") == 0) {//HUAWEI machine
            input_rate_upperbound = 1000000 / 1.0E+09;// events/ns
        } else {
            input_rate_upperbound = 500000 / 1.0E+09;// events/ns
        }
        backPressure = conf.getBoolean("backPressure", false);
    }

    public SchedulingPlan Search(boolean random, int timeoutMs) {
        this.random = random;
        start_time = System.nanoTime();
        SchedulingPlan sp = new SchedulingPlan(topology, numNodes, cons, conf, random);
//		currentPlan = ;
//		if (!currentPlan.success()) {
//			sp.variables.SOURCE_RATE = sp.variables.SOURCE_RATE * 0.9;
//			return Search(worst_plan, timeoutMs);
//		}

        if (!random) {
            return Packing(sp);
        }
        return randomPacking(sp);
    }


    /**
     * @param sp
     * @param parallelism
     * @return the failed executor or null (success).
     */
    private SchedulingPlan schedule(SchedulingPlan sp, Parallelism parallelism) {


        if (conf.getBoolean("NAV", false)) {
            sp = new randomPlan_NoConstraints(sp.graph, numNodes, numCPUs, cons, conf)
                    .Search(false, -1);

        } else if (conf.getBoolean("roundrobin", false)) {
            sp = new roundrobin(sp.graph, numNodes, numCPUs, cons, conf)
                    .Search(false, -1);

        } else if (conf.getBoolean("toff", false)) {
            sp = new TOFF_hardConstraint(sp.graph, numNodes, numCPUs, cons, conf)
                    .Search(false, -1);
        } else {
            sp = new BranchAndBound(sp.graph, numNodes, numCPUs, cons, conf, null)
                    .Search(false, 30000);
        }

        if (sp != null) {
            sp.Parallelism = parallelism;
        }
        return sp;
    }

    /**
     * find a random scaling plan.
     *
     * @param sp
     * @return
     */
    private SchedulingPlan randomPacking(SchedulingPlan sp) {

        TopologyComponent[] array = sp.graph.topology.getRecords().values().toArray(new TopologyComponent[0]);
        List<TopologyComponent> topologyComponentList = Arrays.asList(array);
        Collections.reverse(topologyComponentList);

        Parallelism p = null;
        p = new Parallelism(sp.Parallelism);
        boolean scale_flag = false;

        Random r = new Random();
        while (p.total_num < scale_upperbound) {
            TopologyComponent op = topologyComponentList.get(r.nextInt(topologyComponentList.size()));
            if (op.getOp() == null) {
                continue;
            }

            if (!op.getOp().isScalable()) {
//				LOG.info("Operator " + op.getId() + " is not scalable");
            } else {

                int OverRatio = r.nextInt(scale_upperbound - p.total_num) + 1;
                if (OverRatio >= 1) {
                    LOG.info("Scaling operator:" + op.getId() + "\tscale_ratio:" + OverRatio);
                    scale_flag = scale_executor(op, p, sp, OverRatio);
                    if (!scale_flag) {
                        return new SchedulingPlan(topology, numNodes, cons, p, conf, sp.variables);
                    }
                }

            }

        }
        return new SchedulingPlan(topology, numNodes, cons, p, conf, sp.variables);
    }


    private SchedulingPlan Packing(SchedulingPlan sp) {
        //double original_source_rate = sp.variables.SOURCE_RATE;
        sp = schedule(sp, sp.Parallelism);
        ExecutionNode executionNode;
        if (!sp.success()) {
            executionNode = sp.failed_executor;
        } else {
            executionNode = null;
        }
        if (sp.success()) {
//			LOG.info("Output Rate of new plan (without BP):" + output_rate * 1E6);
            storeBestPlan(sp, true);


            sp.variables.SOURCE_RATE = default_sourceRate / 1E9;//recover source rate.
            sp.cache_clean();
            sp.planToString(false, false);

            boolean scale_flag = false;
            Parallelism p = null;
            p = new Parallelism(sp.Parallelism);
            //TODO:use multiple topological sortted graph to perform the following routing can make sure optimality.
            TopologyComponent[] array = sp.graph.topology.getRecords().values().toArray(new TopologyComponent[0]);
            List<TopologyComponent> topologyComponentList = Arrays.asList(array);
            Collections.reverse(topologyComponentList);


            for (int i = 0; i < topologyComponentList.size(); i++) {
                TopologyComponent op = topologyComponentList.get(i);
                if (op.getOp() == null) {
                    continue;
                }
//				double smallest_ratio = Double.MAX_VALUE;
                if (!op.getOp().isScalable() || op.isLeadNode()) {
                    LOG.info("Operator " + op.getId() + " is not scalable");


//					if (op.isLeadNode()) {
//						if (backPressure) {
//							double original_input_rate = sp.variables.SOURCE_RATE;
//							sp.variables.SOURCE_RATE *= 1.1;
//							LOG.info("Source Input rate increased from: " + original_input_rate + "  to " + sp.variables.SOURCE_RATE);
//							Packing(sp);
//						}
//					}


                } else {
                    for (ExecutionNode e : op.getExecutorList()) {
                        Set<String> s = new HashSet<>(e.operator.input_streams);//remove duplicate input streams.
                        for (String streamId : s) {
                            cache cache = sp.cacheMap.get(e.getExecutorID()).get(streamId);
                            double OverRatio = cache.getInputRate() / cache.getBounded_processRate();
                            if (OverRatio > 1) {
                                LOG.info("Bottleneck operator:" + op.getId() + "\tscale_ratio:" + OverRatio);
                                scale_flag = scale_executor(op, p, sp, OverRatio);
                                if (scale_flag) {
                                    break;
                                }
                            }
                        }
                        if (scale_flag) {
                            break;
                        }
                    }
                }
                if (scale_flag) {
                    break;
                }
            }
            if (!scale_flag) {
                TopologyComponent op = topologyComponentList.get(topologyComponentList.size() - 1);

                LOG.info("no bottleneck operators found, try to increase data source");
                if (op.getOp().isScalable() && op.getNumTasks() < 17) {//only if we allow spout to increase.
                    scale_flag = scale_executor(op, p, sp, 1);
                }

            }
            return helper(scale_flag, new SchedulingPlan(topology, numNodes, cons, p, conf, sp.variables));
        } else {
            assert executionNode != null;
            LOG.info("Executor:" + executionNode + " is failed to be allocated in last round.");
            topology.getRecord(executionNode.getOP()).toCompress = false;
            boolean scale_flag = false;
            Parallelism p = null;
            p = new Parallelism(sp.Parallelism);
            scale_flag = scale_executor(executionNode.operator, p, sp, 1);
            return helper(scale_flag, new SchedulingPlan(topology, numNodes, cons, p, conf, sp.variables));
        }
    }


    private boolean scale_executor(TopologyComponent op, Parallelism p, SchedulingPlan sp, double overRatio) {
//		if (!op.getOp().isScalable()) {
//			LOG.info("Operator " + op.getId() + " is not scalable");
//			return false;
//		}

        Integer num = p.get(op.getId());
//		if (numNodes > 4) {
//			int inc = Math.max(num + 5, (int) (num * overRatio / 5));
//			if (inc + sp.graph.getExecutionNodeArrayList().size() - num <= scale_upperbound) {
//				p.put(op.getId(), inc);
//				LOG.info("Big increase, BasicBoltBatchExecutor " + op.getId() + " will be scaled to:" + inc + " from " + num);
//				return true;
//			} else {
//				inc = num + 2;
//				if (sp.graph.getExecutionNodeArrayList().size() + 10 < scale_upperbound
//						&& inc + sp.graph.getExecutionNodeArrayList().size() - num <= scale_upperbound) {
//					p.put(op.getId(), inc);
//					LOG.info("Middle increase, BasicBoltBatchExecutor " + op.getId() + " will be scaled to:" + inc + " from " + num);
//					return true;
//				} else if (sp.graph.getExecutionNodeArrayList().size() + 1 <= scale_upperbound) {
//					p.put(op.getId(), num + 1);
//					LOG.info("Small increase, BasicBoltBatchExecutor " + op.getId() + " will be scaled to:" + (num + 1) + " from " + num);
//					return true;
//				} else {//stop further scale
//					LOG.info("Executor " + op.getId() + " has been scaled to:" + num + " and cannot be further scaled up.");
//					return false;
//				}
//			}
//		} else {
        final int inc = 1;//= Math.max(1, (int) Math.ceil(overRatio));
        int size = 0;
        size = p.total_num + sp.graph.topology.getRecord(SPOUT).getNumTasks();
        LOG.info("Current graph size:" + size);
        LOG.info("Scale upper bound:" + scale_upperbound);
        if (size + inc <= scale_upperbound) {
            p.increment(op.getId(), inc);//p.put(op.getId(), num + inc);
            LOG.info("Executor " + op.getId() + " will be scaled to:" + (num + inc) + " from " + num);
            return true;
        } else if (size + 1 <= scale_upperbound) {
            p.increment(op.getId(), 1);//p.put(op.getId(), num + 1);
            LOG.info("Executor " + op.getId() + " will be scaled to:" + (num + 1) + " from " + num);
            return true;
        } else {
            LOG.info("Executor " + op.getId() + " has been scaled to:" + num + " and cannot be further scaled up.");
            return false;
        }
//		}
    }

    private SchedulingPlan helper(boolean scale_flag, SchedulingPlan sp) {
        if (!scale_flag) {
            LOG.info("Further scale failed, return the best plan found so far.");
            return best_plan;
        }
//		else if (timeout()) {
//			LOG.info("Time out, further scale failed, return the best plan found so far.");
//			return best_plan;
//		}
        else {
            return Packing(sp);
        }
    }

    private boolean timeout() {
        return System.nanoTime() - start_time > time_out;
    }

    private void storeBestPlan(SchedulingPlan sp, boolean BP) {
        final double output_rate = sp.getOutput_rate(BP);
        if (best_plan != null) {
            if (best_plan.getOutput_rate(BP) < output_rate) {
                best_plan = new SchedulingPlan(sp, true);
                LOG.info("Store best_plan with output rate:" + best_plan.getOutput_rate(BP));
                best_plan.planToString(false, BP);
                for (TopologyComponent tr : topology.getRecords().values()) {
                    tr.toCompress = true;
                }
//				final boolean measure_end = cons.measure_end(best_plan);
//				if (!measure_end) {
//					LOG.info("Constraint measure_end failed!!");
//					cons.show(best_plan);
//				}
            }
        } else {
            best_plan = new SchedulingPlan(sp, true);
            LOG.info("Store best_plan with output rate:" + output_rate);
            best_plan.planToString(false, BP);
        }
    }
}
