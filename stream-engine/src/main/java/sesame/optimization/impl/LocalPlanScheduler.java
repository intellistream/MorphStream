package sesame.optimization.impl;
import common.collections.Configuration;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.model.Constraints;
/**
 * Created by tony on 7/11/2017.
 */
public abstract class LocalPlanScheduler extends PlanScheduler {
    protected Constraints cons;
    protected int numNodes;
    protected int numCPUs;
    protected SchedulingPlan best_plan = null;
    private SchedulingPlan currentPlan;
    private double targetOutput = 0;
    private void input_satisfy() {
    }
    protected int try_allocate(SchedulingPlan sp, ExecutionNode executor) {
        int satisfy = cons.allstatisfy;//by default it is satisfied.
        if (executor.getExecutorID() == -2) {
            sp.setAllocated(executor);
            sp.cache_clean();
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
    private void storeCurrentPlan(ExecutionGraph graph, Configuration conf) {
        SchedulingPlan pre_plan = null;
        if (currentPlan != null && currentPlan.success()) {
            best_plan = new SchedulingPlan(currentPlan, true);//remember current plan
            targetOutput = best_plan.getOutput_rate(true);
            pre_plan = currentPlan = best_plan;
        } else {
            pre_plan = currentPlan = new SchedulingPlan(graph, numNodes, cons, conf, null);
        }
    }
    void initilize(ExecutionGraph graph, Configuration conf, boolean worst_plan) {
        if (worst_plan) {
            targetOutput = Double.MAX_VALUE;
        } else {
            targetOutput = Double.MIN_VALUE;
        }
        storeCurrentPlan(graph, conf);
    }
    public abstract SchedulingPlan Search(boolean worst_plan, int timeoutMs);
}
