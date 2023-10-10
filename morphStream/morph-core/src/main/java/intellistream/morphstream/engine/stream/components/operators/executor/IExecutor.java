package intellistream.morphstream.engine.stream.components.operators.executor;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;

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

    void setExecutionNode(ExecutionNode e);

    int getStage();

    double getEmpty();
}
