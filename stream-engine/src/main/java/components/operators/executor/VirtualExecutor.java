package components.operators.executor;

import components.context.TopologyContext;
import execution.ExecutionNode;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Marker;
import faulttolerance.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import topology.TopologyBuilder;

import java.util.Map;

import static common.CONTROL.enable_log;

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
    public void configureWriter(Writer writer) {
    }

    @Override
    public void clean_state(Marker marker) {
    }

    @Override
    public int getStage() {
        return -1;
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void callback(int callee, Marker marker) {
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
