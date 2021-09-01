package execution;

import common.platform.Platform;
import components.TopologyComponent;
import components.operators.executor.IExecutor;
import components.operators.executor.SpoutExecutor;
import controller.input.InputStreamController;
import controller.output.OutputController;
import execution.runtime.tuple.impl.Marker;
import faulttolerance.Writer;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;
import java.util.Set;

import static common.CONTROL.enable_log;

/**
 * Created by shuhaozhang on 11/7/16.
 *
 * @since 1.0.0, ExecutionNode support multiple input (receive_queue) and output streams (through stream partition).
 */
public class ExecutionNode implements Serializable {
    private static final long serialVersionUID = 4218117262516218310L;
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionNode.class);
    public final TopologyComponent operator;//Operator structure shouldn't be referenced as tp_engine information.....
    //Below are self maintain structures.
    public final IExecutor op;//Operator should be owned by executionNode as it maintains unique information such as context information.
    //model related structures. Source executor Id, related model.
    public final int compressRatio;
    private final int executorID;//global ID for this executorNode in current Brisk.topology
    //    public HashMap<Integer, Boolean> BP=new HashMap<>();//Backpressure corrected.
    private final boolean BP = false;
    private final boolean activate = true;
    //Below are created later.
    private OutputController controller;//TODO: it should be initialized during Brisk.topology compile, which is after current Brisk.execution SimExecutionNode being initialized, so it can't be made final.
    private InputStreamController inputStreamController;//inputStreamController is initialized even after partition.
    private boolean last_executor = false;//if this executor is the last executor of operator
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> parents = new HashMap();
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> children = new HashMap();
    //private boolean _allocated = false;
    private boolean _statistics;
    private boolean first_executor;

    public ExecutionNode(TopologyComponent rec, int i, Platform p, int compressRatio) {
        this.operator = rec;
        this.executorID = i;
        this.compressRatio = compressRatio;
        // create an tp_engine of Operator through serialization.
        // We need to do this because each Brisk.execution thread should own its own context, which is hold by the Operator structure!
        IExecutor op = rec.getOp();
        if (op != null) {
            this.op = SerializationUtils.clone(op);
        } else {
            this.op = null;
        }
        parents = new HashMap();
        children = new HashMap();

    }

    public ExecutionNode(ExecutionNode e, TopologyComponent topo, Platform p) {
        compressRatio = e.compressRatio;
        this.operator = topo;
        this.executorID = e.executorID;
        this.first_executor = e.first_executor;
        this.last_executor = e.last_executor;
        // create an tp_engine of Operator through serialization.
        // We need to do this because each Brisk.execution thread should own its own context, which is hold by the Operator structure!
        IExecutor op = operator.getOp();
//        if(getOP().equalsIgnoreCase(COUNT_VEHICLES_BOLT)){
//            System.nanoTime();
//        }
        if (op != null) {
            this.op = SerializationUtils.clone(op);
        } else {
            this.op = null;
        }
        parents = new HashMap();
        children = new HashMap();

    }

    public ExecutionNode(TopologyComponent rec, int i, Platform p) {
        this.operator = rec;
        this.executorID = i;
        compressRatio = 1;
        // create an tp_engine of Operator through serialization.
        // We need to do this because each Brisk.execution thread should own its own context, which is hold by the Operator structure!
        IExecutor op = rec.getOp();
//        if(getOP().equalsIgnoreCase(COUNT_VEHICLES_BOLT)){
//            System.nanoTime();
//        }
        if (op != null) {
            this.op = SerializationUtils.clone(op);
        } else {
            this.op = null;
        }

    }

    public boolean isFirst_executor() {
        return first_executor;
    }

    public void setFirst_executor(boolean first_executor) {
        this.first_executor = first_executor;
    }

    public String toString() {
        return this.getOP();
    }

    public boolean is_statistics() {
        return _statistics;
    }

    /**
     * custom inputStreamController for this execution mapping_node.
     *
     * @return
     */
    public boolean hasScheduler() {
        return inputStreamController != null;
    }

    public InputStreamController getInputStreamController() {
        return inputStreamController;
    }

    public void setInputStreamController(InputStreamController inputStreamController) {
        this.inputStreamController = inputStreamController;
    }

    public HashMap<TopologyComponent, ArrayList<ExecutionNode>> getParents() {
        return parents;
    }

    public Set<TopologyComponent> getParents_keySet() {
        return parents.keySet();
    }

    public HashMap<TopologyComponent, ArrayList<ExecutionNode>> getChildren() {
        return children;
    }

    public Set<TopologyComponent> getChildren_keySet() {
        return children.keySet();
    }

    public ArrayList<ExecutionNode> getParentsOf(TopologyComponent operator) {
        ArrayList<ExecutionNode> executionNodes = parents.get(operator);
        if (executionNodes == null) {
            executionNodes = new ArrayList<>();
        }
        return executionNodes;
    }

    public String getOP() {
        return operator.getId();
    }

    public String getOP_full() {
        return operator.getId() + "(" + executorID + ")";
    }

    public ArrayList<ExecutionNode> getChildrenOf(TopologyComponent operator) {
        return children.get(operator);
    }

    public OutputController getController() {
        return controller;
    }

    //    /**
//     * TODO: It's too costly to measure_end this flag every time. This is in Brisk.execution critical metric_path!!
//     * Move it to construction phase and Store it locally!
//     */
//    public SPSCController getPc(String streamId, String boltID) {
//
//        return partition.getPartitionController(streamId, boltID);
//    }
    public void setController(OutputController controller) {
        this.controller = controller;
    }

    public int getExecutorID() {
        return executorID;
    }

    public boolean isSourceNode() {
        return operator.getOp() instanceof SpoutExecutor;
    }

    public boolean isLeafNode() {
        return operator.isLeafNode();
    }

    private boolean isLeadNode() {
        final ExecutionNode node = operator.getExecutorList().iterator().next();
        return this == node;
    }

    public boolean isEmpty() {
        return getController() == null || getController().isEmpty();
    }

    public void setReceive_queueOfChildren(String streamId) {
        if (!isLeafNode()) {
            final OutputController controller = getController();
            if (!controller.isShared() || this.isLeadNode()) {
                //for each downstream Operator ID
                if (operator.getChildrenOfStream(streamId) != null) {
                    for (TopologyComponent op : this.operator.getChildrenOfStream(streamId).keySet()) {
                        for (ExecutionNode downstream_executor : op.getExecutorList()) {
                            int executorID = this.getExecutorID();
                            //GetAndUpdate output queue from output partition.
                            final Queue queue = this.getController()
                                    .getPartitionController(streamId, op.getId())
                                    .get_queue(downstream_executor.getExecutorID());
//                        if (enable_log) LOG.info("Set queue for downstream executor:" + downstream_executor);
                            downstream_executor.inputStreamController.setReceive_queue(
                                    streamId,
                                    executorID,//executorId refers the upstream of downstream executor...
                                    queue);
                        }
                    }
                } else {
                    if (enable_log)
                        LOG.info("Executor:" + this.getOP_full() + " have no children! for stream " + streamId);
                }
            }
        }
        if (this.inputStreamController != null) {
            this.inputStreamController.initialize();
        }
    }

    public void allocate_OutputQueue(boolean linked, int desired_elements_epoch_per_core) {
        if (!isLeafNode()) {
            final OutputController controller = getController();
            if (controller.isShared()) {//if we use MPSC.
                if (this.isLeadNode()) {
                    controller.allocatequeue(linked, desired_elements_epoch_per_core);
                }
            } else {
                controller.allocatequeue(linked, desired_elements_epoch_per_core);//if every executor owns its own queue
            }
        }
    }

    public void setLast_executorOfBolt(boolean last_executor) {
        this.last_executor = last_executor;
    }

    //GetAndUpdate partition ratio for the downstream executor
    public boolean isVirtual() {
        return executorID == -2;
    }

    public void display() {
        op.display();
    }

    public void configureWriter(Writer writer) {
        op.configureWriter(writer);
    }

    public void clean_state(Marker marker) {
        op.clean_state(marker);
    }

}