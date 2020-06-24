package sesame.optimization.impl;

import application.util.Configuration;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.model.Constraints;
import sesame.optimization.model.Variables;

/**
 * Created by tony on 7/11/2017.
 */
public abstract class PlanScheduler {

    public Configuration conf;
    protected ExecutionGraph graph;
    protected Constraints cons;
    protected int numNodes;
    protected int numCPUs;
    protected SchedulingPlan currentPlan;
    protected SchedulingPlan best_plan = null;
    protected SchedulingPlan Pre_plan = null;
    protected double targetOutput = 0;

    private void input_satisfy() {

    }

    protected int try_allocate(SchedulingPlan sp, ExecutionNode executor) {
        int satisfy = cons.allstatisfy;//by default it is satisfied.
        if (executor.getExecutorID() == -2) {
            sp.setAllocated(executor);
            return satisfy;
        }
        for (int i = 1; i < numNodes + 1; i++) {
            sp.allocate(executor, i);
            satisfy = cons.satisfy(sp, i);
            if (satisfy != cons.allstatisfy) {
                sp.deallocate(executor);
                sp.cache_clean();
            } else {
                sp.setAllocated(executor);
                sp.cache_clean();
                break;
            }
        }
        return satisfy;
    }

    private void storeCurrentPlan(Configuration conf) {
        if (currentPlan != null && currentPlan.success()) {
            Pre_plan = best_plan = currentPlan;
        } else {
            Pre_plan = best_plan = currentPlan = new SchedulingPlan(graph, numNodes, cons, conf, null);
        }
    }

    private void storeCurrentPlan(Configuration conf, Variables variables) {
        if (currentPlan != null && currentPlan.success()) {
            Pre_plan = best_plan = currentPlan;
        } else {
            Pre_plan = best_plan = currentPlan = new SchedulingPlan(graph, numNodes, cons, conf, variables);
        }
    }


    protected void initilize(boolean worst_plan, Configuration conf) {
        if (worst_plan) {
            targetOutput = Double.MAX_VALUE;
        } else {
            targetOutput = Double.MIN_VALUE;
        }
        storeCurrentPlan(conf);
    }

    protected void initilize(boolean worst_plan, Configuration conf, Variables variables) {
        if (worst_plan) {
            targetOutput = Double.MAX_VALUE;
        } else {
            targetOutput = Double.MIN_VALUE;
        }
        storeCurrentPlan(conf, variables);
    }


    public abstract SchedulingPlan Search(boolean worst_plan, int timeoutMs);
}
