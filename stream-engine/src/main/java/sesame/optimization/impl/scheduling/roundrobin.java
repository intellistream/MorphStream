package sesame.optimization.impl.scheduling;
import common.collections.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.model.Constraints;

import java.util.ArrayList;
import java.util.Iterator;
/**
 * Created by tony on 7/11/2017.
 */
public class roundrobin extends randomPlan_NoConstraints {
    private final static Logger LOG = LoggerFactory.getLogger(roundrobin.class);
    public roundrobin(ExecutionGraph graph, int numNodes, int numCPUs, Constraints cons, Configuration conf) {
        super(graph, numNodes, numCPUs, cons, conf);
        LOG.info("Round robin based scheduling.");
    }
    SchedulingPlan Packing(SchedulingPlan sp, ExecutionGraph graph, ArrayList<ExecutionNode> sort_opList) {
        final Iterator<ExecutionNode> iterator = sort_opList.iterator();
        int s = 0;
        while (iterator.hasNext()) {
            ExecutionNode executor = iterator.next();
            sp.allocate(executor, s++ % numNodes);
        }
        sp.set_success();
        return sp;
    }
}