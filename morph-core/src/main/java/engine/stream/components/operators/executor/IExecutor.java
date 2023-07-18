package engine.stream.components.operators.executor;

import engine.stream.components.context.TopologyContext;
import engine.stream.execution.ExecutionNode;
import engine.stream.execution.runtime.collector.OutputCollector;

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
