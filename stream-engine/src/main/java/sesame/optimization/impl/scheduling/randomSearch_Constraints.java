package sesame.optimization.impl.scheduling;
import common.collections.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.PlanScheduler;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.model.Constraints;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;
import java.util.stream.IntStream;
/**
 * Created by tony on 7/11/2017.
 */
public class randomSearch_Constraints extends PlanScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(randomSearch_Constraints.class);
    public randomSearch_Constraints(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf) {
        this.numNodes = numNodes;
        this.numCPUs = numCPUs;
        this.graph = graph;
        this.cons = cons;
        this.conf = conf;
    }
    public SchedulingPlan Search(boolean worst_plan, int timeoutMs) {
        initilize(worst_plan, conf);
        //if it is not used in set_executor_ready plan..
        if (worst_plan)
            LOG.info("Randomly search for the worst plan");
        else
            LOG.info("Randomly search for the best plan");
        //main course.
        final ArrayList<ExecutionNode> sort_opList = graph.sort();
        final long s = System.currentTimeMillis();
        int threads = Runtime.getRuntime().availableProcessors();
        while (System.currentTimeMillis() - s < timeoutMs) {
            SchedulingPlan[] plans = new SchedulingPlan[threads];
            for (int i = 0; i < threads; i++)
                plans[i] = new SchedulingPlan(currentPlan, true);
            IntStream.range(0, threads).parallel().forEach(i -> {
                plans[i] = Packing(plans[i], plans[i].graph, sort_opList);
                plans[i].getOutput_rate(true);
            });
            if (worst_plan) {//select the worst one
                for (int i = 0; i < threads; i++) {
                    double outputRate = plans[i].getOutput_rate(true);
                    if (outputRate < targetOutput) {
                        LOG.info("worst search, GetAndUpdate plan to:" + outputRate);
                        targetOutput = outputRate;
                        best_plan = plans[i];//GetAndUpdate best plan
//                        Pre_plan = new SchedulingPlan(plans[i], false);//d_record current plan.
//                        currentPlan = best_plan;
                    }
                }
            } else {//select the best one
                for (int i = 0; i < threads; i++) {
                    double outputRate = plans[i].getOutput_rate(true);
                    if (outputRate > targetOutput) {
                        LOG.info("best search, GetAndUpdate plan to:" + outputRate);
                        targetOutput = outputRate;
                        best_plan = plans[i];//GetAndUpdate best plan
//                        Pre_plan = new SchedulingPlan(plans[i], false);//d_record current plan.
//                        currentPlan = best_plan;
                    }
                }
            }
        }
        double output_rate = best_plan.getOutput_rate(true);
        return best_plan;
    }
    SchedulingPlan Packing(SchedulingPlan sp, ExecutionGraph graph, ArrayList<ExecutionNode> sort_opList) {
        final Iterator<ExecutionNode> iterator = sort_opList.iterator();
        Random r = new Random();
        while (iterator.hasNext()) {
            ExecutionNode executor = iterator.next();
            if (!sp.Allocated(executor)) {
                int satisfy = cons.allstatisfy;//by default it is satisfied.
                LinkedList<Integer> valid_places = new LinkedList<>();
                for (int i = 0; i < numNodes; i++) {
                    sp.allocate(executor, i);
                    satisfy = cons.satisfy(sp, i);
                    sp.deallocate(executor);
                    sp.cache_clean();
                    if (satisfy == cons.allstatisfy) {
                        valid_places.add(i);
                    }
                }
                if (valid_places.size() == 0) {
                    LOG.info("Operator\t" + executor.getOP()
                            + "\tis set_failed to allocate, with CPU relax of:" + cons.relax_cpu + "\t, " +
                            "Memory relax of:" + cons.relax_memory
                            + "\t, QPI relax of:" + cons.relax_qpi);
//                    cons.show(graph, sp);
                    cons.adjust_satisfy(satisfy);
                    Packing(sp, graph, sort_opList);//re-packing
                    return sp;
                } else {
                    //randomly select one of the valid places to allocate it.
                    Integer integer = valid_places.get(r.nextInt(valid_places.size()));
                    sp.allocate(executor, integer);
                    sp.setAllocated(executor);
                    sp.cache_clean();
                }
            }
        }
        sp.set_success();
//        sp.relax_cpu = this.relax_cpu;
//        sp.relax_memory = this.relax_memory;
//        sp.relax_qpi = this.relax_qpi;
        return sp;
    }
}