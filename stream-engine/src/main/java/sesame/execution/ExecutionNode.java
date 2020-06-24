package sesame.execution;
import common.Constants;
import common.platform.Platform;
import common.collections.Configuration;
import ch.usi.overseer.OverHpc;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.TopologyComponent;
import sesame.components.context.TopologyContext;
import sesame.components.operators.executor.IExecutor;
import sesame.components.operators.executor.SpoutExecutor;
import sesame.controller.input.InputStreamController;
import sesame.controller.output.OutputController;
import sesame.controller.output.PartitionController;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.faulttolerance.Writer;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.model.RateModel;
import sesame.optimization.model.STAT;
import state_engine.common.OrderLock;
import state_engine.common.OrderValidate;

import java.io.Serializable;
import java.util.*;
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
    public final RateModel RM;
    //model related structures. Source executor Id, related model.
    public final HashMap<Integer, STAT> profiling = new HashMap<>();
    public final int compressRatio;
    private final int executorID;//global ID for this executorNode in current Brisk.topology
    //    public HashMap<Integer, Boolean> BP=new HashMap<>();//Backpressure corrected.
    private final boolean BP = false;
    //Below are created later.
    private OutputController controller;//TODO: it should be initialized during Brisk.topology compile, which is after current Brisk.execution SimExecutionNode being initialized, so it can't be made final.
    private InputStreamController inputStreamController;//inputStreamController is initialized even after partition.
    private boolean last_executor = false;//if this executor is the last executor of operator
    private boolean activate = true;
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> parents = new HashMap();
    private HashMap<TopologyComponent, ArrayList<ExecutionNode>> children = new HashMap();
    //private boolean _allocated = false;
    private boolean _statistics;
    private boolean first_executor;
    private boolean needsProfile;
    public ExecutionNode(TopologyComponent rec, int i, Platform p, int compressRatio) {
        this.operator = rec;
        this.executorID = i;
        this.compressRatio = compressRatio;
        // create an tp_engine of Operator through serialization.
        // We need to do this because each Brisk.execution thread should own its own context, which is hold by the Operator structure!
        IExecutor op = rec.getOp();
        if (op != null) {
            this.op = SerializationUtils.clone(op);
            RM = new RateModel(op, p);
        } else {
            this.op = null;
            RM = new RateModel(null, p);
        }
        parents = new HashMap();
        children = new HashMap();
        needsProfile = false;
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
            RM = new RateModel(op, p);
        } else {
            this.op = null;
            RM = new RateModel(null, p);
        }
        parents = new HashMap();
        children = new HashMap();
        needsProfile = e.needsProfile;
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
            RM = new RateModel(op, p);
        } else {
            this.op = null;
            RM = new RateModel(null, p);
        }
        needsProfile = false;
    }
    public boolean isFirst_executor() {
        return first_executor;
    }
    public void setFirst_executor(boolean first_executor) {
        this.first_executor = first_executor;
    }
    public void setNeedsProfile() {
        this.needsProfile = true;
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
//                        LOG.info("Set queue for downstream executor:" + downstream_executor);
                            downstream_executor.inputStreamController.setReceive_queue(
                                    streamId,
                                    executorID,//executorId refers the upstream of downstream executor...
                                    queue);
                        }
                    }
                } else {
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
    public void setDeactivate() {
        activate = false;
    }
    public boolean isActivate() {
        return activate;
    }
    public boolean isLast_executor() {
        return last_executor;
    }
    public void setLast_executorOfBolt(boolean last_executor) {
        this.last_executor = last_executor;
    }
    public String inputAggregationType(TopologyComponent operator) {
        //TODO: extend this in future.
        return "sum";
    }
    //GetAndUpdate partition ratio for the downstream executor
    public double getPartition_ratio(String input_stream_downOpID, String downOpID, int ExecutorID) {
        if (ExecutorID == -2) {
            return 1;//virtual ground.
        }
        Double partition_ratio = null;
        try {
            partition_ratio = controller.getPartitionController(input_stream_downOpID, downOpID).getPartition_ratio(ExecutorID);
        } catch (Exception e) {
            Collection<PartitionController> partitionControllers = controller.getPartitionController();
            for (PartitionController partitionController : partitionControllers) {
                LOG.info(partitionController.toString());
            }
            System.exit(-1);
        }
        return partition_ratio;
    }
    public boolean noParents() {
        final Set<TopologyComponent> parents_set = this.getParents_keySet();//multiple parent
        return parents_set.size() == 0;
    }
    public void prepareProfilingStruct(Configuration conf, OverHpc hpcMonotor, TopologyContext context, Platform p) {
        if (noParents()) {             //producer
            profiling.put(-1, new STAT(this, this, conf, hpcMonotor, p));
        } else {
            for (TopologyComponent parent : getParents_keySet()) {
                if (conf.getBoolean("profile", false)) {
                    ExecutionNode src = parent.getExecutorList().get((parent.getNumTasks() - 1) / 2);
                    profiling.put(src.getExecutorID(), new STAT(src, this, conf, hpcMonotor, p));
                } else {
                    for (ExecutionNode src : parent.getExecutorList()) {
                        profiling.put(src.getExecutorID(), new STAT(src, this, conf, hpcMonotor, p));
                    }
                }
            }
        }
    }
    /**
     * In terms of latency
     */
    public double getCommConsumption(ExecutionNode parentE, SchedulingPlan plan, boolean bound) {
        double sum = 0;
        if (this.operator.input_streams == null)//SPOUT
        {
            return RM.getCommConsumption(this, parentE, Constants.DEFAULT_STREAM_ID, plan, bound);
        }
        for (String streamId : new HashSet<>(this.operator.input_streams)) {
            if (parentE.operator.getOutput_streams().containsKey(streamId))//make sure this stream is from this parentE.
            {
                sum += RM.getCommConsumption(this, parentE, streamId, plan, bound);
            }
        }
        return sum;
    }
    /**
     * In terms of bandwdith
     *
     * @param parentE
     * @param plan
     * @param bound
     * @return
     */
    public double getTransConsumption(ExecutionNode parentE, SchedulingPlan plan, boolean bound) {
        double sum = 0;
        if (this.operator.input_streams == null)//SPOUT
        {
            return RM.getTransConsumption(this, parentE, Constants.DEFAULT_STREAM_ID, plan, bound);
        }
        for (String streamId : new HashSet<>(this.operator.input_streams)) {
            if (parentE.operator.getOutput_streams().containsKey(streamId))//make sure this stream is from this parentE.
            {
                sum += RM.getTransConsumption(this, parentE, streamId, plan, bound);
            }
        }
        return sum;
    }
    public double getMemConsumption(SchedulingPlan plan, boolean bound) {
        double sum = 0;
        if (this.operator.input_streams == null)//SPOUT
        {
            return RM.getMemConsumption(this, Constants.DEFAULT_STREAM_ID, plan, bound);
        }
        for (String streamId : new HashSet<>(this.operator.input_streams)) {
            sum += RM.getMemConsumption(this, streamId, plan, bound);
        }
        return sum;
    }
    /**
     * @param sp
     * @return the process_rate of current setting.
     */
    public double getUnitCycles(ExecutionNode parentE, String streamId, SchedulingPlan sp, boolean bound) {
        return RM.cycles_scPertuple(this, parentE, streamId, sp, bound);
    }
    public int getdemandCores() {
        assert this.compressRatio > 0;
        return this.compressRatio;
    }
    public double getdemandCycles(SchedulingPlan plan, boolean bound) {
        double sum = 0;
        if (this.operator.input_streams == null)//SPOUT
        {
            return RM.getdemandCycles_c(this, Constants.DEFAULT_STREAM_ID, plan, bound);
        }
        for (String streamId : new HashSet<>(this.operator.input_streams)) {//remove duplicate streams
            sum += RM.getdemandCycles_c(this, streamId, plan, bound);
        }
        return sum;
    }
    public double getdemandCycles(String streamId, SchedulingPlan plan, boolean bound) {
        if (this.operator.input_streams == null)//SPOUT
        {
            return RM.getdemandCycles_c(this, Constants.DEFAULT_STREAM_ID, plan, bound);
        }
//        for (String streamId : this.operator.input_streams)
        return RM.getdemandCycles_c(this, streamId, plan, bound);
    }
    //    public double getInputRate(String streamId, ExecutionNode producer, double sourceRate, SchedulingPlan sp) {
//        return RM.ri_sc(this, producer, streamId, sourceRate, sp);
//    }
    public double getMemConsumption(String streamId, SchedulingPlan plan, boolean bound) {
        double sum = 0;
        if (this.operator.input_streams == null)//SPOUT
        {
            return RM.getMemConsumption(this, Constants.DEFAULT_STREAM_ID, plan, bound);
        }
        return RM.getMemConsumption(this, streamId, plan, bound);
    }
    public double getInputRate(String streamId, SchedulingPlan sp, boolean bound) {
        return RM.ri_c(this, streamId, sp, bound);
    }
    public double getInputRate(String streamId, ExecutionNode producer, SchedulingPlan sp, boolean bound) {
        return RM.ri_sc(this, producer, streamId, sp, bound);
    }
    public double getExpectedProcessRate(String streamId, ExecutionNode producer, SchedulingPlan sp, boolean bound) {
        return RM.erp_sc(this, producer, streamId, sp, bound);
    }
    public double getExpectedProcessRate(String streamId, SchedulingPlan sp, boolean bound) {
        return RM.erp_c(this, streamId, sp, bound);
    }
    public double getBoundedProcessRate(String streamId, SchedulingPlan sp, boolean bound) {
        return RM.brp_c(this, streamId, sp, bound);
    }
    public double getBoundedProcessRate(String streamId, ExecutionNode parentE, SchedulingPlan sp, boolean bound) {
        return RM.brp_sc(this, parentE, streamId, sp, bound);
    }
    //GetAndUpdate outputRate of specific output streams..
    public double getOutputRate(String executionNode_InputStreamId, String executionNode_OnputStreamId, ExecutionNode parent, SchedulingPlan sp, boolean bound) {
        return RM.ro_sc(this, parent, executionNode_InputStreamId, executionNode_OnputStreamId, sp, bound);
    }
    public double getOutputRate(String executionNode_InputStreamId, String executionNode_OnputStreamId, SchedulingPlan sp, boolean bound) {
        return RM.ro_c(this, executionNode_InputStreamId, executionNode_OnputStreamId, sp, bound);
    }
    //	public boolean children_BP_Corrected() {
//		Set<TopologyComponent> children_keySet = this.getChildren_keySet();
//		if (children_keySet.size() == 0) {
//			return true;
//		}
//		for (TopologyComponent child_Op : children_keySet) {
//			for (ExecutionNode child : child_Op.getExecutorList()) {
//				//  if (child.BP.GetAndUpdate(this.getExecutorID())!=null&&!child.BP.GetAndUpdate(this.getExecutorID())) return false;
//				if (!child.BP) {
//					return false;
//				}
//			}
//		}
//		return true;
//	}
    public double CleangetOutput_rate(String executionNode_InputStreamId
            , String executionNode_OnputStreamId, final SchedulingPlan sp, boolean bound) {
        return RM.CleangetOutput_rate(this, executionNode_InputStreamId, executionNode_OnputStreamId, sp, bound);
    }
    /**
     * make sure this executor is not idle for the given producer
     *
     * @param producer
     * @return
     */
    public boolean idle(ExecutionNode producer) {
        for (String outputstreams : producer.operator.getOutput_streamsIds()) {
            for (String inputstreams : new HashSet<>(this.operator.input_streams)) {
                if (outputstreams.equalsIgnoreCase(inputstreams)) {
                    if (this.RM.input_selectivity.get(inputstreams) != 0) {
                        return false;
                    }
                }
            }
        }
        return true;
    }
    public boolean isVirtual() {
        return executorID == -2;
    }
    public void display() {
        op.display();
    }
    public boolean needsProfile() {
        return needsProfile;
    }
    public void configureWriter(Writer writer) {
        op.configureWriter(writer);
    }
    public void configureLocker(OrderLock lock, OrderValidate orderValidate) {
        op.configureLocker(lock, orderValidate);
    }
    public void clean_state(Marker marker) {
        op.clean_state(marker);
    }
    public void earlier_clean_state(Marker marker) {
        op.earlier_clean_state(marker);
    }
}