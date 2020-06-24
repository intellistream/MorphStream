package sesame.optimization.impl.scheduling;

import application.util.Configuration;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.PlanScheduler;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.model.Constraints;
import sesame.optimization.model.Variables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.stream.IntStream;

/**
 * Created by tony on 7/11/2017.
 */
public class randomSearch_hardConstraints extends PlanScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(randomSearch_hardConstraints.class);
    private final Variables variables;

    public randomSearch_hardConstraints(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf, Variables variables) {
        this.variables = variables;
        this.numNodes = numNodes;
        this.numCPUs = numCPUs;
        this.graph = graph;
        this.cons = cons;
        this.conf = conf;

    }

    public SchedulingPlan Search(boolean worst_plan, int timeoutMs) {
        initilize(worst_plan, conf, variables);
        //if it is not used in set_executor_ready plan..
        //LOG.DEBUG("Randomly search for the best plan");
        //main course.
//		final ArrayList<ExecutionNode> sort_opList = graph.sort();
        final long s = System.currentTimeMillis();

        int threads = Runtime.getRuntime().availableProcessors();
        SchedulingPlan[] plans = new SchedulingPlan[threads];
        SchedulingPlan best_plan = null;//local best plan used just for this search method
        while (System.currentTimeMillis() - s < timeoutMs) {
            for (int i = 0; i < threads; i++) {
                plans[i] = new SchedulingPlan(currentPlan, true);
            }

            IntStream.range(0, threads).parallel().forEach(i -> {
                plans[i] = Packing(plans[i], plans[i].graph.getExecutionNodeArrayList());
                plans[i].getOutput_rate(true);
            });

            for (int i = 0; i < threads; i++) {
                double outputRate = plans[i].outputrate;
                if (outputRate > targetOutput) {
                    //LOG.DEBUG("best search, GetAndUpdate plan to:" + outputRate);
                    targetOutput = outputRate;
                    best_plan = plans[i];//GetAndUpdate best plan
//                        Pre_plan = new SchedulingPlan(plans[i], false);//d_record current plan.
//                        currentPlan = best_plan;
                }
            }

            if (best_plan == null) {
                LOG.info("failed to find any better plan");
                best_plan = plans[0];
            }
        }
        //double output_rate = best_plan.getOutput_rate();
        return best_plan;
    }

    private SchedulingPlan
    Packing(SchedulingPlan sp, ArrayList<ExecutionNode> sort_opList) {
        Random r = new Random();
        int satisfy = cons.allstatisfy;//by default it is satisfied.
        for (ExecutionNode executor : sort_opList) {//if all are allocated then exit.
//			ExecutionNode executor = Operation.GetAndUpdate(r.nextInt(Operation.size()));//randomly pick one executor to proceed.
//			if (sp.Allocated(executor)) {
//				LOG.info("Something wrong in the algorithm!");
//			}

            //pin spout thread to one socket.

//			if (executor.isSourceNode()) {
//				sp.allocate(executor, 1);
//				continue;
//			}


            //consider all the possible ways to allocate it.
            LinkedList<Integer> l = new LinkedList<>();

            for (int i = 0; i < numNodes; i++) {

                sp.allocate(executor, i);
                if (executor.isVirtual()) {
                    satisfy = cons.allstatisfy;
                } else {
                    satisfy = cons.satisfy(sp, i);
                }
                sp.deallocate(executor);
                if (satisfy == cons.allstatisfy) {
                    l.add(i);

//					sp.allocate(executor, i);
//					final boolean measure_end = cons.measure_end(sp);
//					if (!measure_end) {
//						LOG.info("Something wrong in the algorithm!");
//						LOG.info(cons.show(sp));
//					}
//					sp.deallocate(executor);
                }
            }

            if (l.size() == 0) {
                LOG.debug("Operator\t" + executor.getOP() + "\tis set_failed to allocate, with CPU relax of:" + cons.relax_display() + "\tsatisfying " + cons.constraintBy(satisfy));
                sp.failed_executor = executor;//set failed executor
                return sp;
            } else {
                //randomly select one of the valid places to allocate it.
                Integer integer = l.get(r.nextInt(l.size()));
                sp.allocate(executor, integer);
                sp.setAllocated(executor);
                sp.cache_clean();

//				final boolean measure_end = cons.measure_end(sp);
//				if (!measure_end) {
//					LOG.info("Something wrong in the algorithm!");
//					LOG.info(cons.show(sp));
//				}

            }
        }

//		final boolean measure_end = cons.measure_end(sp);
//		if (!measure_end) {
//			LOG.info("Something wrong in the algorithm!");
//			LOG.info(cons.show(sp));
//		}

        sp.set_success();
        return sp;
    }
}