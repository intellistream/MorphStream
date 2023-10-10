package intellistream.morphstream.engine.stream.components.operators.api;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.OutputFieldsDeclarer;

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

    /**
     * This is the API to client application code.
     * This can be overwrite by specific operator to do some initialization work.
     *
     * @param thread_Id
     * @param thisTaskId
     * @param graph
     */
    void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph);

}
