package intellistream.morphstream.engine.stream.components.operators.executor;

import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionNode;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.JumboTuple;
import intellistream.morphstream.engine.stream.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static intellistream.morphstream.configuration.CONTROL.enable_log;

public class VirtualExecutor implements IExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);
    private static final long serialVersionUID = 6833979263182987686L;

    //AbstractBolt op;
    public VirtualExecutor() {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
//		op.prepare(stormConf, context, collector);
    }

    @Override
    public int getID() {
        return -1;
    }

    @Override
    public String getConfigPrefix() {
        return null;
    }

    @Override
    public TopologyContext getContext() {
        return null;
    }

    @Override
    public void display() {
    }

    @Override
    public double getResults() {
        return 0;
    }

    @Override
    public int getStage() {
        return -1;
    }

    @Override
    public void setExecutionNode(ExecutionNode e) {
    }

    public void execute(JumboTuple in) throws InterruptedException {
        if (enable_log) LOG.info("Should not being called.");
    }

    public boolean isStateful() {
        return false;
    }

    public double getEmpty() {
        return 0;
    }
}
