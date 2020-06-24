package sesame.optimization.routing;

import sesame.components.TopologyComponent;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.SchedulingPlan;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by I309939 on 11/8/2016.
 */

class Variable_list extends LinkedList {
    private static final long serialVersionUID = -5807395681735317165L;

    public Variable getVariables(int source, int dest) {
        for (Object o : this) {
            Variable v = (Variable) o;
            if (v.source_label == source && v.dest_label == dest) {
                return v;
            }
        }
        return null;
    }
}

class Variable {
    public final int source_label;//belongs to which mapping_node.
    public final int dest_label;//belongs to which mapping_node.

    public Variable(int source_label, int dest_label, String var_name) {
        this.source_label = source_label;
        this.dest_label = dest_label;
    }

}

public class RoutingOptimizer {
    private final ExecutionGraph g;
    private final Variable_list VarList = new Variable_list();

    public RoutingOptimizer(ExecutionGraph g) {

        this.g = g;
    }

    //build variable list without h constraint function.
    private void buildVarListwithout_h() {
        for (ExecutionNode mn : g.getExecutionNodeArrayList()) {
            for (TopologyComponent children : mn.getChildren().keySet()) {
                for (ExecutionNode dst : children.getExecutorList()) {
                    VarList.add(new Variable(mn.getExecutorID(), dst.getExecutorID()
                            , String.format("P_%s_%s", mn.getExecutorID(), dst.getExecutorID())));
                }
            }
        }
    }

    private double get_numerator(ArrayList<Double> array, int i) {
        double rt = 1;
        for (int j = 0; j < array.size(); j++) {
            if (j == i) continue;
            rt *= array.get(j);
        }
        return rt;
    }

//    /**
//     * take executor speed into account to do local optimal partition
//     *
//     * @param plan
//     * @param executionNode
//     */
//    private void GetAndUpdate(SchedulingPlan plan, ExecutionNode executionNode) {
//
//
//        for (TopologyComponent children : executionNode.getChildren().keySet()) {
//
//            for (String streamId : executionNode.operator.getOutput_streams().keySet()) {
//                SPSCController partition =
//                        executionNode.getController().getPartitionController(streamId, children.getId());
//
//                ArrayList<Double> cycles = new ArrayList<>();
//
//                if (children.getExecutorList().size() < 2) continue;
//
//                cycles.addAll(children.getExecutorList().stream().map(dst -> dst.getUnitCycles(executionNode, plan)).collect(Collectors.toList()));
//
//                int i = 0;
//                for (ExecutionNode dst : children.getExecutorList()) {
//                    Double nominator = cycles.GetAndUpdate(i);
//                    double sum = 0;
//                    for (int j = 0; j < cycles.size(); j++) {
//                        sum += nominator / cycles.GetAndUpdate(j);
//                    }
//                    partition.partition_ratio.put(dst.getExecutorID(), 1 / sum);
//                    i++;
//                }
//            }
//        }
//    }
//

    /**
     * TODO: implement this in next version
     *
     * @param plan
     * @param executionNode
     */
    private void update_partition(SchedulingPlan plan, ExecutionNode executionNode) {
//        if (executionNode.isSourceNode()) {//terminate case
//            GetAndUpdate(plan, executionNode);
//        } else {
//            GetAndUpdate(plan, executionNode);
//            for (TopologyComponent op : executionNode.getParents_keySet())
//                for (ExecutionNode parent : op.getExecutorList())
//                    update_partition(plan, parent);
//        }
    }

    /**
     * currently use local optimal way to optimize the routing.
     *
     * @param RP
     * @return
     */
    public RoutingPlan optimize(RoutingPlan RP) {
        ExecutionNode virtual = g.getvirtualGround();
        update_partition(RP.schedulingPlan, virtual);
        RP.updateExtendedTargetId();
        return RP;
    }
}
