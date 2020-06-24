package sesame.optimization.model;

import application.Constants;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.SchedulingPlan;


/**
 * Created by I309939 on 11/8/2016.
 */
public class GraphMetrics {
    private final ExecutionNode virtualGround;
    private final SchedulingPlan schedulingPlan;
    private final boolean backPressure;

    public GraphMetrics(SchedulingPlan schedulingPlan, boolean backPressure) {
        this.backPressure = backPressure;
        ExecutionGraph graph = schedulingPlan.graph;
        virtualGround = graph.getvirtualGround();
        this.schedulingPlan = schedulingPlan;
    }

    public double getOutput_rate(boolean bound) {
        if (backPressure && !schedulingPlan.BP_calculated) {
            BackPressure.BP(schedulingPlan);
        }
        return virtualGround.CleangetOutput_rate(Constants.DEFAULT_STREAM_ID, Constants.DEFAULT_STREAM_ID, schedulingPlan, bound);
    }
}
