package sesame.execution;
import common.collections.Configuration;
import common.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.MultiStreamComponent;
import sesame.components.Topology;
import sesame.components.TopologyComponent;
import sesame.components.grouping.Grouping;
import sesame.components.operators.executor.VirtualExecutor;
import sesame.controller.input.InputStreamController;
import sesame.controller.input.scheduler.SequentialScheduler;
import sesame.controller.input.scheduler.UniformedScheduler;
import sesame.controller.output.MultiStreamOutputContoller;
import sesame.controller.output.OutputController;
import sesame.controller.output.PartitionController;
import sesame.controller.output.partition.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static common.Constants.virtualType;
/**
 * Created by shuhaozhang on 11/7/16.
 */
public class ExecutionGraph extends RawExecutionGraph {
    private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);
    private static final long serialVersionUID = 1435918828573882042L;
    public final Topology topology;//make sure no reference to original topology. Each executionGraph should has its own Topology correspondingly.
    private final InputStreamController global_tuple_scheduler;
    private final Configuration conf;
    //	public LinkedList<ExecutionNode> bottleneck = new LinkedList<>();
    private ExecutionNode virtualNode;
    private ExecutionNode spout;
    private ExecutionNode sink;
    private boolean shared;//share by multi producers
    private boolean common;//shared by mutlti consumers
    /**
     * creates execution graph from topology
     *
     * @param topo
     * @param parallelism
     * @param conf
     */
    public ExecutionGraph(Topology topo, Map<String, Integer> parallelism, Configuration conf) {
        this.conf = conf;
        this.topology = new Topology(topo);//copy the topology
        this.topology.clean_executorInformation();
        this.global_tuple_scheduler = topology.getScheduler();
        Configuration(this.topology, parallelism, conf, topology.getPlatform());
    }
    /**
     * duplicate graph
     *
     * @param graph
     * @param conf
     */
    public ExecutionGraph(ExecutionGraph graph, Configuration conf) {
        this.conf = conf;
        topology = new Topology(graph.topology);
        topology.clean_executorInformation();
        global_tuple_scheduler = graph.global_tuple_scheduler;
        Configuration(graph, conf, topology.getPlatform());
    }
    /**
     * creates the compressed graph
     *
     * @param graph
     * @param compressRatio
     * @param conf
     */
    public ExecutionGraph(ExecutionGraph graph, int compressRatio, Configuration conf) {
        this.conf = conf;
        topology = graph.topology;
        topology.clean_executorInformation();
        global_tuple_scheduler = graph.global_tuple_scheduler;
        if (compressRatio == -1) {
            LOG.info("automatically decide the compress ratio");
            compressRatio = (int) Math.max(5, Math.ceil(graph.getExecutionNodeArrayList().size() / 36.0));
        }
        LOG.info("Creates the compressed graph with compressRatio:" + compressRatio);
        Configuration(topology, compressRatio, conf, topology.getPlatform());
    }
    private void Configuration(ExecutionGraph graph, Configuration conf, Platform p) {
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            if (!e.isVirtual()) {
                addRecord(e, topology.getRecord(e.operator.getId()), p);
            }
        }
        spout = executionNodeArrayList.get(0);
        sink = executionNodeArrayList.get(executionNodeArrayList.size() - 1);
        setup(conf, p);
    }
    private void Configuration(Topology topology, int compressRatio, Configuration conf, Platform p) {
        for (TopologyComponent tr : topology.getRecords().values()) {
            if (!tr.isLeadNode() && tr.toCompress) {
                addRecord_RebuildRelationships(tr, compressRatio, p);
            } else {
                if (!tr.toCompress) {
                    LOG.info("Exe:" + tr.getId() + "not able to allocate in last round, do not compress it.");
                }
                addRecord(tr, p);
            }
        }
        setup(conf, p);
    }
    private void Configuration(Topology topology, Map<String, Integer> parallelism, Configuration conf, Platform p) {
        if (parallelism != null) {
            for (String tr : parallelism.keySet()) {
                TopologyComponent record = topology.getRecord(tr);
                record.setNumTasks(parallelism.get(tr));   //reset num of Tasks specified in operator.
            }
        }
        for (TopologyComponent tr : topology.getRecords().values()) {
            addRecord(tr, p);
        }
        setup(conf, p);
    }
    /**
     * this will set up partition partition (also the partition ratio)
     *
     * @param conf
     * @param p
     */
    private void setup(Configuration conf, Platform p) {
        shared = conf.getBoolean("shared", false);
        common = conf.getBoolean("common", false);
        final ArrayList<ExecutionNode> executionNodeArrayList = getExecutionNodeArrayList();
        spout = executionNodeArrayList.get(0);
        sink = executionNodeArrayList.get(executionNodeArrayList.size() - 1);
        virtualNode = addVirtual(p);
        for (ExecutionNode executor : executionNodeArrayList) {
            if (executor.isLeafNode()) {
                continue;
            }
            final TopologyComponent operator = executor.operator;
            for (String stream : operator.get_childrenStream()) {
                add(operator.getChildrenOfStream(stream).keySet(), executor, operator);
            }
        }
        for (ExecutionNode par : sink.operator.getExecutorList()) {
            par.getChildren().put(virtualNode.operator, new ArrayList<>());
            par.getChildrenOf(virtualNode.operator).add(virtualNode);
            virtualNode.getParents().putIfAbsent(sink.operator, new ArrayList<>());
            virtualNode.getParentsOf(sink.operator).add(par);
        }
        build_streamController(conf.getInt("batch", 100));

    }
    public ExecutionNode getSpout() {
        return spout;
    }
    public ExecutionNode getSink() {
        return sink;
    }
    private void add(Set<TopologyComponent> children, ExecutionNode executor, TopologyComponent operator) {
        for (TopologyComponent child : children) {
            executor.getChildren().putIfAbsent(child, new ArrayList<>());
            final ArrayList<ExecutionNode> childrenExecutor = child.getExecutorList();
            for (ExecutionNode childExecutor : childrenExecutor) {
                executor.getChildrenOf(child).add(childExecutor);
                childExecutor.getParents().putIfAbsent(operator, new ArrayList<>());
                childExecutor.getParentsOf(operator).add(executor);
            }
        }
    }
    //partition writer - multiple reader situation, volatile is enough.
    public ExecutionNode getvirtualGround() {
        return virtualNode;
    }
    public InputStreamController getGlobal_tuple_scheduler() {
        return global_tuple_scheduler;
    }
    private ExecutionNode addVirtual(Platform p) {
        MultiStreamComponent virtual = new MultiStreamComponent("Virtual", virtualType, new VirtualExecutor(), 1, null, null, null);
        ExecutionNode virtualNode = new ExecutionNode(virtual, -2, p);
        addExecutor(virtualNode);
        virtual.link_to_executor(virtualNode);//creates Operator->executor link.
        return virtualNode;
    }
    private void addRecord_RebuildRelationships(TopologyComponent operator, int compressRatio, Platform p) {
        if (compressRatio < 1) {
            LOG.info("compressRatio must be greater than 1, and your setting:" + compressRatio);
            System.exit(-1);
        }
//		int numTasks = 0;
        //create executionNode and assign unique vertex id to it.
        for (int i = 0; i < operator.getNumTasks() / compressRatio; i++) {
            //creates executor->Operator link
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p, compressRatio);//Every executionNode obtain its unique vertex id..
            if (i == operator.getNumTasks() - 1) {
                vertex.setLast_executorOfBolt(true);
            }
            if (i == 0) {
                vertex.setFirst_executor(true);
            }
            addExecutor(vertex);
            operator.link_to_executor(vertex);//creates Operator->executor link.
//			numTasks++;
        }
        //left-over
        int left_over = operator.getNumTasks() % compressRatio;
        if (left_over > 0) {
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p, left_over);//Every executionNode obtain its unique vertex id..
            vertex.setLast_executorOfBolt(true);
            addExecutor(vertex);
            operator.link_to_executor(vertex);//creates Operator->executor link.
        }
//		for (int i = 0; i < operator.getNumTasks() % compressRatio; i++) {
//			//creates executor->Operator link
//			ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p, 1);//Every executionNode obtain its unique vertex id..
//			if (i == operator.getNumTasks() - 1) {
//				vertex.setLast_executorOfBolt(true);
//			}
//			addExecutor(vertex);
//			operator.link_to_executor(vertex);//creates Operator->executor link.
////			numTasks++;
//		}
//		operator.numTasks = numTasks;//GetAndUpdate numTasks.
    }
    /**
     * @param e
     * @param topo     is the corresponding operator associated in e.
     * @param platform
     * @return
     */
    private ExecutionNode addRecord(ExecutionNode e, TopologyComponent topo, Platform platform) {
        //creates executor->Operator link
        ExecutionNode vertex = new ExecutionNode(e, topo, platform);//Every executionNode obtain its unique vertex id..
        addExecutor(vertex);
        topo.link_to_executor(vertex);//creates Operator->executor link.
        return vertex;
    }
    private void addRecord(TopologyComponent operator, Platform p) {
        //create executionNode and assign unique vertex id to it.
        for (int i = 0; i < operator.getNumTasks(); i++) {
            //creates executor->Operator link
            ExecutionNode vertex = new ExecutionNode(operator, vertex_id++, p);//Every executionNode obtain its unique vertex id..
            if (i == operator.getNumTasks() - 1) {
                vertex.setLast_executorOfBolt(true);
            }
            if (i == 0) {
                vertex.setFirst_executor(true);
                if (conf.getBoolean("profile", false)) {
                    vertex.setNeedsProfile();//use the first executor as the profiling target.
                }
            }
            addExecutor(vertex);
            operator.link_to_executor(vertex);//creates Operator->executor link.
        }
    }
    /**
     * Initialize partition partition of each downstream srcOP.
     *
     * @param streamId
     * @param srcOP
     * @param batch
     * @param executor
     * @param common
     * @return
     */
    private HashMap<String, PartitionController> init_pc(String streamId, TopologyComponent srcOP, int batch, ExecutionNode executor, boolean common) {
        //<DownOp, PC>
        HashMap<String, PartitionController> PClist = new HashMap<>();
        //final ArrayList<TopologyComponent> childrenOP;
        if (srcOP.getChildrenOfStream(streamId) != null) {
            for (TopologyComponent childOP : srcOP.getChildrenOfStream(streamId).keySet()) {
                HashMap<Integer, ExecutionNode> downExecutor_list = new HashMap<>();
                for (ExecutionNode e : childOP.getExecutorList()) {
                    downExecutor_list.put(e.getExecutorID(), e);
                }
                PClist.put(childOP.getId(), partitionController_create(srcOP, childOP, streamId, downExecutor_list, batch, executor, common));//if in-order output enabled.
            }
        }
        return PClist;
    }
    /**
     * This is used only for SPSC.
     *
     * @param srcOP
     * @param childOP
     * @param streamId
     * @param downExecutor_list
     * @param batch
     * @param executor
     * @param common
     * @return
     */
    private PartitionController partitionController_create(TopologyComponent srcOP, TopologyComponent childOP
            , String streamId, HashMap<Integer, ExecutionNode> downExecutor_list, int batch, ExecutionNode executor, boolean common) {
        Grouping g = srcOP.getGrouping_to_downstream(childOP.getId(), streamId);
        if (g.isMarkerShuffle()) {
            return new MarkerShufflePartitionController(srcOP, childOP, this.getSink().getExecutorID()
                    , downExecutor_list, batch, executor, common, conf.getBoolean("profile", false), conf);
        } else if (g.isShuffle()) {
            return new ShufflePartitionController(srcOP, childOP
                    , downExecutor_list, batch, executor, common, conf.getBoolean("profile", false), conf);
        } else if (g.isFields()) {
            return new FieldsPartitionController(srcOP, childOP
                    , downExecutor_list, srcOP.get_output_fields(streamId), g.getFields(), batch, executor, common, conf.getBoolean("profile", false), conf);
        } else if (g.isGlobal()) {
            return new GlobalPartitionController(srcOP, childOP
                    , downExecutor_list, batch, executor, common, conf.getBoolean("profile", false), conf);
        } else if (g.isAll()) {
            return new AllPartitionController(srcOP, childOP
                    , downExecutor_list, batch, executor, common, conf.getBoolean("profile", false), conf);
        } else if (g.isPartial()) {
            return new PartialKeyGroupingController(srcOP, childOP
                    , downExecutor_list, srcOP.get_output_fields(streamId), g.getFields(), batch, executor, common, conf.getBoolean("profile", false), conf);
        } else {
            LOG.info("Create partition controller error: not supported yet!");
            return null;
        }
    }
    void build_inputScheduler() {
        for (ExecutionNode executor : executionNodeArrayList) {
            if (executor.isSourceNode()) {
                continue;
            }
            //Each thread has its own Brisk.execution.runtime.tuple scheduler, which can be customize for each bolt.
            if (!executor.hasScheduler()) {
                //use global scheduler instead.
                // executor.setInputStreamController(new Scheduler(global_tuple_scheduler));//every executor owns its own input scheduler.
                if (global_tuple_scheduler instanceof SequentialScheduler) {
                    executor.setInputStreamController(new SequentialScheduler());
                } else if (global_tuple_scheduler instanceof UniformedScheduler) {
                    executor.setInputStreamController(new UniformedScheduler());
                } else {
                    LOG.error("Unknown input scheduler!");
                }
            }
        }
    }
    private void build_streamController(int batch) {
        if (!shared) {
            for (ExecutionNode executor : executionNodeArrayList) {//Build sc for each executor who is not leaf mapping_node.
                if (executor.isLeafNode()) {
                    continue;
                }
                // streamId, DownOpId, PC
                HashMap<String, HashMap<String, PartitionController>> PCMaps = new HashMap<>();
                for (String streamId : executor.operator.getOutput_streamsIds()) {
                    HashMap<String, PartitionController> PClist = init_pc(streamId, executor.operator, batch, executor, common);//unique PC
                    PCMaps.put(streamId, PClist);
                }
                OutputController sc = new MultiStreamOutputContoller((MultiStreamComponent) executor.operator, PCMaps);
                executor.setController(sc);
            }
        } else {
            for (TopologyComponent operator : topology.getRecords().values()) {
                if (operator.isLeafNode()) {
                    continue;
                }
                // streamId, DownOpId, PC
                HashMap<String, HashMap<String, PartitionController>> PCMaps = new HashMap<>();
                for (String streamId : operator.getOutput_streamsIds()) {
                    HashMap<String, PartitionController> PClist = init_pc(streamId, operator, batch, null, common);//shared PC
                    PCMaps.put(streamId, PClist);
                }
                OutputController sc = new MultiStreamOutputContoller((MultiStreamComponent) operator, PCMaps);
                sc.setShared();
                for (ExecutionNode executor : operator.getExecutorList()) {//double measure_end if they are refer to the same executor
                    executor.setController(sc);
                }
            }
        }
    }
    public Integer getSinkThread() {
        return sink.getExecutorID();
    }
    /**
     * Think about how to implement DAG sort in future..
     *
     * @return
     */
    public ArrayList<ExecutionNode> sort() {
        return getExecutionNodeArrayList();
    }
    /**
     * search for the first execution node that has the same operator with the input e.
     *
     * @param e
     */
    public int getExecutionNodeIndex(ExecutionNode e) {
        final String op = e.getOP();
        for (int i = 0; i < this.getExecutionNodeArrayList().size(); i++) {
            final ExecutionNode node = getExecutionNode(i);
            if (node.getOP().equalsIgnoreCase(op)) {
                return i;
            }
        }
        return -1;//failed to find.
    }
}
