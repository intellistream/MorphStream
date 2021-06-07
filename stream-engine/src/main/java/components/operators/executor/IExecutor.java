package components.operators.executor;

import components.context.TopologyContext;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Marker;
import faulttolerance.Writer;

import java.io.Serializable;
import java.util.Map;

public interface IExecutor extends Serializable {
    long serialVersionUID = 8L;

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector);

    int getID();

    String getConfigPrefix();

    TopologyContext getContext();

    void display();

    double getResults();

    /**
     * Called when an executor is going to be shutdown. There is no guarentee that cleanup
     * will be called, because the supervisor kill -9's worker processes on the cluster.
     */
    void cleanup();

    void callback(int callee, Marker marker);

    void setExecutionNode(ExecutionNode e);

    void configureWriter(Writer writer);

    void clean_state(Marker marker);

    int getStage();

    double getEmpty();
}
