package components.operators.executor;

import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;

import java.io.Serializable;
import java.util.Map;

public interface IExecutor extends Serializable {
    long serialVersionUID = 8L;

    void prepare(Map stormConf, TopologyContext context, OutputCollector collector) throws DatabaseException;

    int getID();

    String getConfigPrefix();

    TopologyContext getContext();

    void display();

    double getResults();

    void setExecutionNode(ExecutionNode e);

    int getStage();

    double getEmpty();
}
