package components.operators.api;
import components.context.TopologyContext;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.OutputFieldsDeclarer;

import java.io.Serializable;
import java.util.Map;
/**
 * Created by shuhaozhang on 12/7/16.
 */
public interface IOperator extends Serializable {
    /**
     * configure output fields through pass in a outputfields declarer.
     */
    void declareOutputFields(OutputFieldsDeclarer declarer);
    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);
    void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph);
    void cleanup();
    void callback(int callee, Marker marker);
}
