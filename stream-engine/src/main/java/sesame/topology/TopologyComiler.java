package sesame.topology;
import common.collections.Configuration;
import sesame.components.Topology;
import sesame.execution.ExecutionGraph;
/**
 * Created by shuhaozhang on 11/7/16.
 */
class TopologyComiler {
    //TODO: It is possible to have a static-lanuch here. For example, code-generation techniques.
    public ExecutionGraph generateEG(Topology topology, Configuration conf) {
        //Construct Brisk.execution Graph structure based on information from this Brisk.topology.
        return new ExecutionGraph(topology, null, conf);
    }
}
