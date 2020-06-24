package sesame.components.operators.executor;

import application.util.Configuration;
import sesame.components.context.TopologyContext;
import sesame.execution.ExecutionNode;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.faulttolerance.Writer;
import sesame.topology.TopologyBuilder;
import state_engine.common.OrderLock;
import state_engine.common.OrderValidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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
    public double get_read_selectivity() {
        return 0;
    }

    @Override
    public Map<String, Double> get_input_selectivity() {
        return null;
    }

    @Override
    public Map<String, Double> get_output_selectivity() {
        return null;
    }

    @Override
    public double get_branch_selectivity() {
        return 0;
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
    public double getLoops() {
        return 0;
    }

    @Override
    public boolean isScalable() {
        return false;
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;
    }

    @Override
    public void configureWriter(Writer writer) {

    }

    public void configureLocker(OrderLock lock, OrderValidate orderValidate) {

    }

    @Override
    public void clean_state(Marker marker) {

    }

    @Override
    public int getStage() {
        return -1;
    }

    @Override
    public void earlier_clean_state(Marker marker) {

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
        LOG.info("Should not being called.");
    }

    public boolean IsStateful() {
        return false;
    }

    public void forceStop() {

    }

    public boolean isStateful() {
        return false;
    }

    public double getEmpty() {
        return 0;
    }
}
