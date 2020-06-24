package sesame.optimization.impl.scheduling;

import application.util.Configuration;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.PlanScheduler;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.model.Constraints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;


/**
 * Created by tony on 3/20/2017.
 */
public class TOFF extends PlanScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(TOFF.class);
    private final Configuration conf;
    //    double relax_cpu;
//    double relax_memory;
//    double relax_qpi;

    public TOFF(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf) {
        this.conf = conf;
        this.numNodes = numNodes;
        this.numCPUs = numCPUs;
        this.graph = graph;
        this.cons = cons; //this.relax_cpu = this.relax_memory = this.relax_qpi = relax;//set default relax
        LOG.info("Toff based scheduling");
    }

    //TO-FF: topological ordered first fit algorithm.
    public SchedulingPlan Search(boolean worst_plan, int timeoutMs) {
        initilize(worst_plan, conf);
        //main course.
        final ArrayList<ExecutionNode> sort_opList = graph.sort();
        final long s = System.currentTimeMillis();

        double output_rate;

        currentPlan = FF_Packing(currentPlan, sort_opList);
        //At this moment, currentplan's cacheMap is correct.
        output_rate = currentPlan.getOutput_rate(true);

        if (output_rate > targetOutput) {
            targetOutput = output_rate;
            best_plan = new SchedulingPlan(currentPlan, false);//GetAndUpdate best plan
            Pre_plan = new SchedulingPlan(currentPlan, false);//d_record current plan.
        } else {
            currentPlan = new SchedulingPlan(Pre_plan, false);
        }
        best_plan.outputrate = output_rate;
        return best_plan;
    }

    private SchedulingPlan FF_Packing(SchedulingPlan sp, ArrayList<ExecutionNode> sort_opList) {

        for (ExecutionNode executor : sort_opList) {
            //            if(executor.getOP().equalsIgnoreCase(ACCOUNT_BALANCE_BOLT_NAME)){
//                LOG.info("");
//            }
            if (!sp.Allocated(executor)) {
                int satisfy = try_allocate(sp, executor);
                if (!sp.Allocated(executor)) {
                    LOG.info("Input rate:" + sp.variables.SOURCE_RATE + "\tOperator:" + executor.getOP()
                            + "\tis set_failed to allocate：" + cons.constraintBy(satisfy));
                    LOG.info(cons.relax_display());
                    boolean adjust_satisfy = cons.adjust_satisfy(satisfy);
                    if (adjust_satisfy) {
                        sp.cache_clean();
                    }
                    sp.ini_allocationMap();
                    FF_Packing(sp, sort_opList);//re-packing
                    return sp;
                }
            }
        }
        sp.set_success();
        return sp;
    }


    //    void initial_allocation(ArrayList<ExecutionNode> sort_opList) {
//        final Iterator<ExecutionNode> iterator = sort_opList.iterator();
//        while (iterator.hasNext()) {
//            ExecutionNode executor = iterator.next();
//            executor.set_deallocated();
//        }
//    }
}
