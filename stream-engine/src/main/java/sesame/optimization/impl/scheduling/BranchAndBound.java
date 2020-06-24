package sesame.optimization.impl.scheduling;

import application.util.Configuration;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.TopologyComponent;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.Decision;
import sesame.optimization.impl.PlanScheduler;
import sesame.optimization.impl.SchedulingPlan;
import sesame.optimization.model.Constraints;
import sesame.optimization.model.cache;

import java.util.*;
import java.util.stream.IntStream;

//import javax.ws.rs.NotSupportedException;


/**
 * collocation decision
 */

class Node implements Comparable {
    private static final Logger LOG = LoggerFactory.getLogger(Node.class);
    List<Decision> decisions;
    SchedulingPlan plan;
    double output_rate;//output rate of this node. maybe bound output rate or actual.. depends on whether this node is valid allocated.

    //   int newAllocate;
    Node() {

    }

    public Node(Node e) {
        this.decisions = new LinkedList<>(e.decisions);
        this.plan = new SchedulingPlan(e.plan, false);
        this.setValidationMap(new HashMap<>(e.getValidationMap()));//duplicates the validationMap.
        this.setValidOperators(e.getvalidOperators());
        this.output_rate = e.output_rate;
    }

    public Node(List<Decision> decisions, Map<Integer,
            Boolean> validationMap, SchedulingPlan plan) {
        this.decisions = new LinkedList<>(decisions);
        this.plan = new SchedulingPlan(plan, false);
        this.setValidationMap(new HashMap<>(validationMap));//duplicates the validationMap.
        this.setValidOperators(plan.validOperators);
        this.output_rate = plan.getBound_rate();//bounded function

        //enabled in debug mode
//		if (!measure_end(validationMap, this.getvalidOperators())) {
//			LOG.error("Something wrong here, Type anything to continue");
//			Scanner scanner = new Scanner(System.in);
//			scanner.nextLine();
//		}
    }

    public Map<Integer, Boolean> getValidationMap() {
        return plan.validationMap;
    }

    public void setValidationMap(Map<Integer, Boolean> validationMap) {
        plan.validationMap = validationMap;
    }

    public int getvalidOperators() {
        return plan.validOperators;
    }

    private void setValidOperators(int validOperators) {
        plan.validOperators = validOperators;//setValidOperators
    }


    private boolean check(Map<Integer, Boolean> validationMap, int validOperators) {
        int sum = 0;
        for (boolean b : validationMap.values()) {
            if (b) {
                sum++;
            }
        }
        return sum == validOperators;
    }

    @Override
    public int compareTo(Object obj) {
        Node emp = (Node) obj;
        //compare decisionList
        Collection subtract = CollectionUtils.subtract(this.decisions, emp.decisions);
        if (subtract.size() != 0) {
            return -1;
        }
        //compare validationMap
        for (int i : this.getValidationMap().keySet()) {
            if (this.getValidationMap().get(i) != emp.getValidationMap().get(i)) {
                return -1;
            }
        }
        //compare schedulingPlan
        if (this.plan.compareTo(emp.plan) != 0) {
            return -1;
        }
        if (this.getvalidOperators() != emp.getvalidOperators()) {
            return -1;
        }
        if (this.output_rate != emp.output_rate) {
            return -1;
        }
        return 0;
    }

//	public void validOperators_inc() {
//		plan.validOperators++;
//	}
}

class Node_Stack extends Stack<Node> {

    private static final long serialVersionUID = 7228068721836919471L;
}

/**
 * Search for optimal placement based on B&B.
 */
public class BranchAndBound extends PlanScheduler {
    private final static Logger LOG = LoggerFactory.getLogger(BranchAndBound.class);
    private final ExecutionGraph original_graph;
    private Node solution_node;
    private Node node;
    private Node_Stack stack;
    private boolean restores = false;
    private boolean single_restores = false;

    public BranchAndBound(ExecutionGraph graph, int numNodes,
                          int numCPUs, Constraints cons, Configuration conf, SchedulingPlan scaling_plan) {
        this.conf = conf;
        this.numNodes = numNodes;
        this.numCPUs = numCPUs;
        this.original_graph = new ExecutionGraph(graph, conf);
        //BnB use compressed graph for fast searching
        this.graph = new ExecutionGraph(graph, conf.getInt("compressRatio", -1), conf);//set_executor_ready topology
        LOG.info("original graph size:" + original_graph.getExecutionNodeArrayList().size());
        LOG.info("compressed graph size:" + this.graph.getExecutionNodeArrayList().size());
        this.cons = cons;
        solution_node = new Node();

        if (scaling_plan != null && scaling_plan.success()) {
            currentPlan = scaling_plan;
        }
    }

    private Map<Integer, Boolean> initialValidation(ExecutionGraph graph) {
        Map<Integer, Boolean> validationMap = new HashMap<>();
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            //if (e.operator.type == spoutType || e.isVirtual()) {
            if (e.isVirtual() || e.isSourceNode()) {//do not consider spout and virtual.
                validationMap.put(e.getExecutorID(), true);
            } else {
                validationMap.put(e.getExecutorID(), false);
            }
        }
        return validationMap;
    }

    private List<Decision> initialAllDecisions(ExecutionGraph graph) {
        List<Decision> decisionList = new LinkedList<>();
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            if (e.isVirtual()) {//do not consider spout and virtual.
                continue;
            }
            if (e.op.get_read_selectivity() == 0) {
                decisionList.add(new Decision(null, e));
            }
            for (TopologyComponent children : e.getChildren().keySet()) {
                for (ExecutionNode e_c : children.getExecutorList()) {
                    if (e_c.op.get_read_selectivity() != 0) {
                        decisionList.add(new Decision(e, e_c));
                    }
                }
            }
        }
        return decisionList;
    }


    public SchedulingPlan Search(boolean worst_plan, int timeoutMs) {
        initilize(worst_plan, conf);
        double initialOutputRate = 0;
        double bk_sourceRate = currentPlan.variables.SOURCE_RATE;

        if (currentPlan.success()) {
            LOG.info("Use previously obtained plan as set_executor_ready plan");
            solution_node.plan = currentPlan;
            solution_node.output_rate = solution_node.plan.outputrate;

        } else {
            PlanScheduler initialScheduler;
            initialScheduler = new randomSearch_hardConstraints(graph, numNodes, numCPUs, cons, conf, null);

            boolean tss = true;
            if (tss) {
                long start = System.nanoTime();
                solution_node.plan = initialScheduler.Search(worst_plan, timeoutMs / 2);//50% of BnB for initial search.
                long end = System.nanoTime();
                LOG.info("It takes " + String.format("%.2f", (end - start) / 1E9) + " seconds to finish initialScheduler searching");
                solution_node.output_rate = solution_node.plan.getOutput_rate(true);
            } else {
                solution_node.output_rate = 0;
            }
        }
        if (solution_node.plan != null && solution_node.plan.success()) {
            initialOutputRate = solution_node.output_rate;
            LOG.info("=============Initial plans:===============\t" + initialOutputRate * 1E6);
            solution_node.plan.finilize();
//			solution_node.plan.planToString(false);
        } else {
            LOG.info("Failed to find any feasible plan");
            timeoutMs = 0;//do not need to further BnB.
        }
        cons.backup();
        cons.relax_reset();
        currentPlan.variables.SOURCE_RATE = bk_sourceRate;

        long start = System.nanoTime();

        SchedulingPlan plan = BnBSearching(timeoutMs);

        long end = System.nanoTime();
        LOG.info("It takes " + String.format("%.2f", (end - start) / 1E9) + " seconds to finish branch and bound searching");
        if (!plan.success()) {
            LOG.info("=============BnB failed===============");
//			solution_node.plan.planToString(false);
            LOG.info("CPU Relax:" + cons.relax_cpu + "\tMemory Relax:" + cons.relax_memory + "\tQPI Relax:" + cons.relax_qpi + "\tCores Relax:" + cons.relax_cores);
        } else {
            LOG.info("=============BnB plans:===============");
//			solution_node.plan.planToString(false);
            LOG.info("BnB output rate (input_event/ms):" + solution_node.plan.getOutput_rate(true) * 1E6 + " initialScheduler output rate (input_event/ms):" + initialOutputRate * 1E6);
//			LOG.info("CPU Relax:" + cons.relax_cpu + "\tMemory Relax:" + cons.relax_memory + "\tQPI Relax:" + cons.relax_qpi + "\tCores Relax:" + cons.relax_cores);
        }
        final SchedulingPlan decompression_plan = new SchedulingPlan(plan, original_graph);//convert graph back.
        return decompression_plan;//return the plan for further process.
    }

    public SchedulingPlan Search(ExecutionGraph graph) {
        return Search(false, 0);
    }


    private SchedulingPlan BnBSearching(int timeoutMs) {
        //LOG.DEBUG("BnB allowed time (sec):" + timeoutMs / 1000 / 1E3);
        node = new Node();
        stack = new Node_Stack();
        boolean improvedByBB = false;
        long end;

        node.decisions = initialAllDecisions(graph);
        node.plan = new SchedulingPlan(graph, numNodes, cons, conf, currentPlan.variables).AllLocal(graph);
        node.setValidationMap(initialValidation(graph));

//		for (ExecutionNode executionNode : graph.getExecutionNodeArrayList()) {
//			if (executionNode.operator.type == spoutType || executionNode.isVirtual()) {
//				node.plan.allocate(executionNode, 0);
//			}
//		}
        node.output_rate = node.plan.getBound_rate();
        LOG.info("======Bound output rate:=======\t" + node.output_rate * 1E6);
        conf.put("bound", node.output_rate * 1E6);


        if (node.output_rate < solution_node.output_rate) {
            LOG.info("Bounded output rate is smaller than set_executor_ready plan's output rate??");
            node.plan.planToString(false, true);
        }

        stack.push(node);

        long start = System.currentTimeMillis();
        while (!stack.isEmpty()) {
            node = stack.pop();//the output rate of node is already updated during the construction of the node.
//                LOG.info("solution_node.output_rate:" + solution_node.output_rate + "\tnode.outputRate:" + node.output_rate + "\t node.validOperators:" + node.validOperators);
            if (node.getvalidOperators() == graph.getExecutionNodeArrayList().size()) {

                if (node.output_rate > solution_node.output_rate/*|| (solution_node.getvalidOperators() == 0 && !restores)*/) {
                    final double org_output_rate = solution_node.output_rate;
                    solution_node = new Node(node);
                    solution_node.plan.set_success();
//					solution_node.plan.cache_clean();
                    improvedByBB = true;
                    LOG.info("A better solution has been found by BnB:"
                            + solution_node.output_rate * 1E6 + ",\toriginal solution:"
                            + org_output_rate * 1E6 + ",\tbounded solution:"
                            + conf.getInt("bound") + "\t stack size:" + stack.size());
                }
            } else if (node.output_rate > solution_node.output_rate) {
                Clean(node);//eliminate un-relevant decisions.
                Expand(node);
            }

            end = System.currentTimeMillis();
            double timeElaspedMS = (end - start);

            if (timeElaspedMS > timeoutMs) {//timeElaspedSEC> timeoutMs / 1E3) {//timeoutMs / 1E3) {
                LOG.info("BnB takes too long: " + timeElaspedMS + " ms, now force to exist." + "\tcurrent nodes's output rate:"
                        + node.output_rate * 1E6 + "\tstack size:"
                        + stack.size() + "#validOperators:"
                        + node.getvalidOperators());
                if (improvedByBB) {
                    LOG.info("BnB takes too long, and valid solution_node has been found! so we break.");
                    break;//
                } else {//if (node.getvalidOperators() >= node.getValidationMap().size() - 2) {//allows up to 2 operators being ``non-scheduled"
//					LOG.info("Try tail allocation");
//					for (Integer e_id : node.getValidationMap().keySet()) {
//						if (!node.getValidationMap().GetAndUpdate(e_id)) {
//							LOG.info("Executor:" + e_id + " is still invalid");
//							node.plan.deallocate(e_id);
//						}
//					}
//					//try to finish allocating the rest non-scheduled operators.
//					final ArrayList<ExecutionNode> sort_opList = graph.sort();
//					final SchedulingPlan plan = Packing(node.plan, sort_opList);
//
//					if (plan.success() && plan.getOutput_rate(true) > solution_node.output_rate) {
//						LOG.info("Tail allocation success");
//						solution_node.plan = plan;
//					} else {
//						if (solution_node.plan.success()) {
//							LOG.info("Tail allocation success, return solution node.");
//							return solution_node.plan;
//						}
//						return end_process();
//					}
//					break;

                    if (!solution_node.plan.success()) {
                        return end_process();
                    } else {
                        return solution_node.plan;
                    }
                }
            }
        }
        return solution_node.plan;
    }

    private SchedulingPlan end_process() {
        LOG.info("All efforts are failed, figure out the bottleneck operators and try to scale it up.");
        double largest_ratio = 1;
        for (ExecutionNode e : node.plan.graph.getExecutionNodeArrayList()) {

            if (e.operator.isLeadNode() || e.operator.isLeafNode()) {//don't scale spout and sink
                continue;
            }

            Set<String> s = new HashSet<>(e.operator.input_streams);//remove duplicate input streams.
            for (String streamId : s) {
                cache cache = node.plan.cacheMap.get(e.getExecutorID()).get(streamId);
                double OverRatio = Math.ceil(cache.getInputRate() * 1E6) / Math.ceil(cache.getBounded_processRate() * 1E6);//what is the over ratio?
                if (OverRatio > largest_ratio) {
                    largest_ratio = OverRatio;
                    node.plan.failed_executor = e;
                }
            }
        }
        if (node.plan.failed_executor == null) {
            LOG.info("No bottleneck executor found, set failed executor to one of the remaining executors");
            for (ExecutionNode e : node.plan.graph.getExecutionNodeArrayList()) {
                if (e.operator.isLeadNode() || e.operator.isLeafNode()) {//don't scale spout and sink
                    continue;
                }
                if (!node.getValidationMap().get(e.getExecutorID())) {
                    LOG.info("Executor:" + e.getExecutorID() + " is still invalid");
                    node.plan.failed_executor = e;//set failed executor
                }
            }
        }
        return node.plan;
    }

    private SchedulingPlan Packing(SchedulingPlan sp, ArrayList<ExecutionNode> sort_opList) {
        final Iterator<ExecutionNode> iterator = sort_opList.iterator();


//		HashMap<ExecutionNode, LinkedList<Integer>> map = new HashMap<>();//all combinations.
        while (iterator.hasNext()) {
            ExecutionNode executor = iterator.next();
            if (!sp.Allocated(executor)) {
                int satisfy = cons.allstatisfy;//by default it is satisfied.
                List<Integer> distinctNodes = new LinkedList<>();
                IntStream.range(0, numNodes).filter(s -> !identical(node, distinctNodes, s)).forEach(distinctNodes::add);

                for (int i : distinctNodes) {
                    sp.allocate(executor, i);
                    satisfy = cons.satisfy(sp, i);
                    if (satisfy == cons.allstatisfy) {
                        break;
                    } else {
                        sp.deallocate(executor);
                    }
                }

                if (satisfy != cons.allstatisfy) {
                    LOG.info("Operator\t" + executor.getOP()
                            + "\t is set_failed to allocate: " + satisfy);
                    sp.set_failed();
                    return sp;
                }
            }
        }
        //TODO: here, we have found all combinations to allocate the rest non-scheduled operators.
        //find the best one out of them?

//		allocate_and_calculate(graph.getExecutionNodeArrayList().size(), node.plan, map);
        sp.set_success();
        return sp;
    }

    private void allocate_and_calculate(int size, SchedulingPlan plan, HashMap<ExecutionNode, LinkedList<Integer>> map) {
        for (ExecutionNode e : map.keySet()) {
            SchedulingPlan new_plan = new SchedulingPlan(plan, false);
            new_plan.validationMap = new HashMap<>(plan.validationMap);
            new_plan.validOperators = plan.validOperators;
            allocate_and_calculate(size, new_plan, e, map);
        }
    }

    private void allocate_and_calculate(int totalOperators, SchedulingPlan plan, ExecutionNode executionNode, HashMap<ExecutionNode, LinkedList<Integer>> map) {

        double max = 0;
        final LinkedList<Integer> integers = map.get(executionNode);
        for (Integer integer : integers) {
            int to_socket = integer;
            SchedulingPlan new_plan = new SchedulingPlan(plan, false);
            new_plan.validationMap = new HashMap<>(plan.validationMap);
            new_plan.validOperators = plan.validOperators;
            new_plan.valid_allocate(executionNode, to_socket);

            for (ExecutionNode e : map.keySet()) {
                if (!new_plan.validationMap.get(e.getExecutorID())) {
                    SchedulingPlan new_new_plan = new SchedulingPlan(new_plan, false);
                    new_new_plan.validationMap = new HashMap<>(new_plan.validationMap);
                    new_new_plan.validOperators = new_plan.validOperators;
                    allocate_and_calculate(totalOperators, new_new_plan, e, map);
                    //max = max > rt ? max : rt;
                }
            }
            if (new_plan.validOperators == totalOperators) {//terminate case
                new_plan.set_success();
                final double output_rate = new_plan.getOutput_rate(true);
                if (best_plan.outputrate < output_rate) {
                    best_plan = new_plan;
                }

            }
        }
    }


    /**
     * remove unnecessary decisions
     *
     * @param e
     */
    private void Clean(Node e) {
        List<Decision> cleaned = new ArrayList<>();
        for (Decision decision : e.decisions) {
            ExecutionNode producer = decision.producer;
            ExecutionNode consumer = decision.consumer;
            if (producer != null) {
                if (!(e.getValidationMap().get(producer.getExecutorID()) && e.getValidationMap().get(consumer.getExecutorID()))) {
                    cleaned.add(decision);
                }
            } else if (!e.getValidationMap().get(consumer.getExecutorID())) {
                cleaned.add(decision);
            }
        }
//		LOG.info("Decision shrink from:" + node.decisions.size() + "to:" + cleaned.size());
        e.decisions = cleaned;
    }

    private void pushAll(Node_Stack stack, Map<Integer, List<Node>> children) {

        for (List<Node> nodeList : children.values()) {
            for (Node node : nodeList) {
                stack.push(node);
            }
        }
    }

    private boolean Expand(Node e) {
        Map<Integer, List<Node>> children = ChildrenOf(e);

        if (children != null) {
//			//**converging faster
//			//	children.sort(Comparator.comparingDouble(Node::getvalidOperators));//increasing order
//			children.sort(new Comparator<Node>() {
//				@Override
//				public int compare(Node o1, Node o2) {
//					return (int) (o1.output_rate * 1E9 - o2.output_rate * 1E9);
//				}
//			});
//			final double first = children.GetAndUpdate(0).output_rate;
//			final double last = children.GetAndUpdate(children.size() - 1).output_rate;
//			LOG.info("children size: " + children.size());
            pushAll(stack, children);
            return true;
        } else {
            return false;
        }
    }

    /**
     * if the newly created node is already in the cache, we shall not add it into children.
     *
     * @param cache
     * @param new_node
     * @return
     */
    private boolean repeated(List<Node> cache, Node new_node) {
        for (Node c : cache) {
            if (c.compareTo(new_node) == 0) {
                return true;
            }
        }
        return false;
    }

    private void CreateDoublePlans(SchedulingPlan plan, List<Decision> sub_decisionList, Node cur, List<Node> children) {
        if (plan != null) {
            Node c = new Node(sub_decisionList, cur.getValidationMap(), plan);//collocating nodes.
            children.add(c);
        }
    }

    /**
     * @param plan
     * @param sub_decisionList
     * @param cur
     * @param children
     * @return
     */
    private boolean CreateSinglePlans(SchedulingPlan plan, List<Decision> sub_decisionList,
                                      Node cur, List<Node> children) {
        if (plan != null) {
            Node node = new Node(sub_decisionList, cur.getValidationMap(), plan);
            children.add(node);
            return true;
        }
        return false;
    }

    private boolean branching_body(Node cur, Decision dec, List<Decision> sub_decisionList, List<Node> children, List<Integer> distinctNodes) {


        if (dec.producer == null) { //cases that consumer does not care as no NUMA.
            for (int s : distinctNodes) {
                cur = new Node(node);
                if (!cur.getValidationMap().get(dec.consumer.getExecutorID())) {
                    SchedulingPlan plan1 = cur.plan.valid_allocate(dec.consumer, s);//cur.plan.validators has already being updated.
                    CreateSinglePlans(plan1, sub_decisionList, cur, children);
                }
            }

        } else {
            if (bothReDeterminable(cur.getValidationMap(), dec)) {
                // double_plans.computeIfAbsent(dec,k-> new LinkedList<>());
                SchedulingPlan plan1 = cur.plan.valid_collocation(cur.getValidationMap(), dec);
                CreateDoublePlans(plan1, sub_decisionList, cur, children);
                cur = new Node(node);//re-new
                SchedulingPlan plan2 = cur.plan.valid_collocation(cur.getValidationMap(), dec);
                CreateDoublePlans(plan2, sub_decisionList, cur, children);
            } else {
                for (int s : distinctNodes) {
                    cur = new Node(node);
                    if (!cur.getValidationMap().get(dec.producer.getExecutorID())) {
                        SchedulingPlan plan1 = cur.plan.valid_allocate(dec.producer, s);//cur.plan.validators has already being updated.
                        CreateSinglePlans(plan1, sub_decisionList, cur, children);
                    }
                    cur = new Node(node);
                    if (!cur.getValidationMap().get(dec.consumer.getExecutorID())) {
                        SchedulingPlan plan2 = cur.plan.valid_allocate(dec.consumer, s);
                        CreateSinglePlans(plan2, sub_decisionList, cur, children);
                    }
                }
            }
        }

        return !children.isEmpty();
    }

    private Map<Integer, List<Node>> parallel_branching(List<Decision> decisionList, List<Integer> distinctNodes) {
        Map<Integer, List<Node>> global_children = new HashMap<>();

        IntStream.range(0, decisionList.size()).parallel().forEach(
                i -> {
                    Node cur = new Node(node);
                    Decision dec = decisionList.get(i);
                    ArrayList<Decision> sub_decisionList;
                    sub_decisionList = new ArrayList<>(decisionList);
                    sub_decisionList.remove(i);
                    List<Node> children = new LinkedList<>();
                    if (branching_body(cur, dec, sub_decisionList, children, distinctNodes)) {
                        global_children.put(i, children);
                    }
                });
//		List<Node> nodeList = null;
//		if (global_children.size() != 0) {
//			nodeList = covertMaptoList(global_children);
//		}
        return global_children;
    }

    private List<Node> parallel_branching2(List<Decision> decisionList, List<Integer> distinctNodes) {
        List<Node> Flat_children =
                Collections.synchronizedList(new LinkedList<>());

//		LOG.info("decisionList.size():" + decisionList.size());
        IntStream.range(0, decisionList.size()).parallel().forEach(
                i -> {
                    Node cur = new Node(node);
                    Decision dec = decisionList.get(i);
                    ArrayList<Decision> sub_decisionList;
                    sub_decisionList = new ArrayList<>(decisionList);
                    sub_decisionList.remove(i);
//					List<Decision> sub_decisionList = new ArrayList<>();
//					sub_decisionList.addAll(decisionList.subList(0, i));
//					sub_decisionList.addAll(decisionList.subList(i + 1, decisionList.size()));
                    branching_body(cur, dec, sub_decisionList, Flat_children, distinctNodes);
                }
        );
        return Flat_children;
    }

    private List<Node> sequential_branching(List<Decision> decisionList, List<Integer> distinctNodes) {
        List<Node> Flat_children = new LinkedList<>();

        for (int i = 0; i < decisionList.size(); i++) {
            Node cur = new Node(node);
            Decision dec = decisionList.get(i);
            List<Decision> sub_decisionList = new ArrayList<>();
            sub_decisionList.addAll(decisionList.subList(0, i));
            sub_decisionList.addAll(decisionList.subList(i + 1, decisionList.size()));
            if (!branching_body(cur, dec, sub_decisionList, Flat_children, distinctNodes)) {
                return null;
            }
        }
        return Flat_children;
    }

    /**
     * In each level of children, there are many-many repeated decisions.
     * Try to eliminate them.
     *
     * @param e
     * @return
     */
    private Map<Integer, List<Node>> ChildrenOf(Node e) {

        //refine decisions here: remove all decisions that are no longer useful (involves already valid operators)

        List<Decision> decisionList = e.decisions;
        if (decisionList.isEmpty()) {
            return null;
        }

        // select only one form those "Identical Compute Nodes"
        List<Integer> distinctNodes =
                new ArrayList<>();

        IntStream.range(0, numNodes).filter(s -> !identical(node, distinctNodes, s)).forEach(distinctNodes::add);
//		long start = System.nanoTime();
        Map<Integer, List<Node>> nodeList = parallel_branching(decisionList, distinctNodes);
//        List<Node> nodeList = sequential_branching(decisionList, distinctNodes);
//		long end = System.nanoTime();
//		//LOG.DEBUG("parallel branch takes (seconds):" + String.format("%.2f", (end - start) / 1E9));
        return nodeList;
    }

    private boolean identical(Node e, List<Integer> distinctNodes, int s) {
        for (int a : distinctNodes) {
            if (this.cons.identical_Nodes(e.getValidationMap(), e.plan, a, s)) {
                return true;
            }
        }
        return false;
    }

    private List<Node> covertMaptoList(Map<Integer, List<Node>> children) {
        List<Node> c =
                Collections.synchronizedList(new ArrayList<Node>());
        children.values().parallelStream().forEach(c::addAll);
        return c;
    }

    private boolean parentsDetermined(Map<Integer, Boolean> validationMap, ExecutionNode executionNode) {
        for (TopologyComponent topo : executionNode.getParents().keySet()) {
            for (ExecutionNode parent : executionNode.getParentsOf(topo)) {
                if (!validationMap.get(parent.getExecutorID())) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean parentsDetermined(Map<Integer, Boolean> validationMap, ExecutionNode srcNode, ExecutionNode executionNode) {
        for (TopologyComponent topo : executionNode.getParents().keySet()) {
            for (ExecutionNode parent : executionNode.getParentsOf(topo)) {
                if (parent != srcNode && !validationMap.get(parent.getExecutorID())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * the nodes must haven't been allocated, but their parents must all be allocated.
     *
     * @param validationMap
     * @param decision
     * @return
     */
    private boolean bothReDeterminable(Map<Integer, Boolean> validationMap, Decision decision) {
        ExecutionNode producer = decision.producer;
        ExecutionNode consumer = decision.consumer;
        return !validationMap.get(producer.getExecutorID()) && !validationMap.get(consumer.getExecutorID()) && parentsDetermined(validationMap, producer) && parentsDetermined(validationMap, producer, consumer);
    }
}
