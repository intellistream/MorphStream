package sesame.optimization.impl;

import application.Constants;
import application.util.Configuration;
import application.util.OsUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.Topology;
import sesame.components.TopologyComponent;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.scaling.Parallelism;
import sesame.optimization.model.Constraints;
import sesame.optimization.model.GraphMetrics;
import sesame.optimization.model.Variables;
import sesame.optimization.model.cache;
import sesame.util.myIntegerMap;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static application.Constants.*;

//import brisk.optimization.txn.scheduling.Decision;

/**
 * Created by I309939 on 11/8/2016.
 */
public class SchedulingPlan implements Comparable<SchedulingPlan>, Serializable {
    private static final long serialVersionUID = -8335014669242344503L;
    private transient final static Logger LOG = LoggerFactory.getLogger(SchedulingPlan.class);
    public final ExecutionGraph graph;//make sure each plan has its own executionGraph, which has its own Topology!!!
    private final Constraints cons;
    final private int numNodes;
    private final Map<Integer, Boolean> allocationMap = new HashMap<>();//records which executor is allocated.
    private final myIntegerMap<Integer> mapping = new myIntegerMap<>();//executor (Integer) to socket (Integer) mapping.
    private final LinkedList<Integer> core_mapping = new LinkedList<>();//executor (Integer) to core (Integer) mapping.
    private final Configuration conf;
    public Parallelism Parallelism = new Parallelism();
    public Map<Integer, Map<String, cache>> cacheMap = new HashMap<>();//BasicBoltBatchExecutor, /*input*/ stream, cache.
    public double outputrate = -1;
    public Variables variables = new Variables();
    public ExecutionNode failed_executor = null;
    public Map<Integer, Boolean> validationMap;
    public int validOperators;//used in BnB
    public boolean BP_calculated = false;
    private boolean allocated;

    public SchedulingPlan(ExecutionGraph graph, int numNodes, Constraints cons, Configuration conf, Variables variables) {
        if (variables != null) {
            this.variables = variables;
        }
        this.graph = graph;
        this.numNodes = numNodes;
        this.cons = cons;
        this.conf = conf;
        allocated = false;
        //initialize allocationMap
        ini_allocationMap();
        ini_cacheMap();
    }

    /**
     * reverse the compressed graph to normal.
     */
    public SchedulingPlan(SchedulingPlan currentPlan, ExecutionGraph original_graph) {
        this.graph = original_graph;
        this.failed_executor = currentPlan.failed_executor;
        this.conf = currentPlan.conf;
        this.numNodes = currentPlan.numNodes;
        this.cons = currentPlan.cons;
        this.allocated = currentPlan.allocated;
        variables = new Variables(currentPlan.variables);
        if (currentPlan.success()) {
            //reproduce the map.
            for (TopologyComponent tr : currentPlan.graph.topology.getRecords().values()) {
                final Integer[] allocated_socket = new Integer[original_graph.topology.getRecord(tr.getId()).getNumTasks()];

                int cnt = 0;
                for (ExecutionNode e : tr.getExecutorList()) {
                    final int to_socket = currentPlan.allocation_decision(e);
                    for (int i = 0; i < e.compressRatio; i++) {
                        allocated_socket[cnt++] = to_socket;
                    }
                }

                for (int i = 0; i < allocated_socket.length; i++) {
                    final Integer to_socket = allocated_socket[i];
                    allocate(original_graph.topology.getRecord(tr.getId()).getExecutorList().get(i), to_socket);
                }
            }

            allocate(original_graph.getvirtualGround(), 0);
            ini_cacheMap();
        }
    }

    /**
     * creates a primitive execution graph for this txn plan..
     *
     * @param topology
     * @param numNodes
     * @param cons
     * @param conf
     * @param random
     */
    public SchedulingPlan(Topology topology, int numNodes, Constraints cons, Configuration conf, boolean random) {
//        ExecutionGraph graph1;
        topology.clean_executorInformation();
        this.numNodes = numNodes;
        this.cons = cons;
        this.conf = conf;
        for (String topo : topology.getRecords().keySet()) {
            if (!topology.getRecord(topo).getOp().isScalable()) {
                continue;
            }
            if (!random) {
                Parallelism.increment(topo, topology.getRecord(topo).getOp().default_scale(conf));
            } else {
                Parallelism.increment(topo, 1);
            }
        }
        graph = new ExecutionGraph(topology, Parallelism, conf);
        allocated = false;
        ini_allocationMap();
        ini_cacheMap();
    }

    public SchedulingPlan(Topology topology, int numNodes, Constraints cons, Parallelism parallelism, Configuration conf, Variables variables) {
        if (variables != null) {
            this.variables = variables;
        }
        this.numNodes = numNodes;
        this.cons = cons;
        this.conf = conf;
        this.Parallelism = parallelism;
        topology.clean_executorInformation();
        graph = new ExecutionGraph(topology, Parallelism, conf);
        allocated = false;
        ini_allocationMap();
        ini_cacheMap();
    }

    /**
     * @param currentPlan
     * @param copy        duplicate graph.
     */
    public SchedulingPlan(SchedulingPlan currentPlan, boolean copy) {
        this.conf = currentPlan.conf;
        this.numNodes = currentPlan.numNodes;
        this.cons = currentPlan.cons;
        this.allocated = currentPlan.allocated;
        this.failed_executor = currentPlan.failed_executor;
        for (int i : currentPlan.mapping.keySet()) {
            mapping.put(i, currentPlan.mapping.get(i));
        }
        for (int i : currentPlan.allocationMap.keySet()) {
            allocationMap.put(i, currentPlan.allocationMap.get(i));
        }

        if (copy) {
            variables = new Variables(currentPlan.variables);
            graph = new ExecutionGraph(currentPlan.graph, currentPlan.conf);//duplicate this graph.
//			variables = new Variables(currentPlan.variables);
//			graph = new ExecutionGraph(currentPlan.graph.topology, Parallelism, currentPlan.conf);
        } else {
            variables = currentPlan.variables;
            this.graph = currentPlan.graph;
        }
        ini_cacheMap();
    }

    public static SchedulingPlan deserializeMyself(Configuration conf, int numNodes) throws
            IOException {
        String directory = System_Plan_Path + OsUtils.OS_wrapper("sesame")
                + OsUtils.OS_wrapper(conf.getConfigPrefix())
                + OsUtils.OS_wrapper(String.valueOf(numNodes))
                + OsUtils.OS_wrapper(String.valueOf(conf.getDouble("gc_factor", 3)));

        FileInputStream f;
        if (conf.getBoolean("random", false)) {
            f = new FileInputStream(new File(directory + OsUtils.OS_wrapper("random.plan")));
        } else if (conf.getBoolean("toff", false)) {
            f = new FileInputStream(new File(directory + OsUtils.OS_wrapper("ff.plan")));
        } else if (conf.getBoolean("roundrobin", false)) {
            f = new FileInputStream(new File(directory + OsUtils.OS_wrapper("rr.plan")));
        } else if (conf.getBoolean("worst", false)) {
            f = new FileInputStream(new File(directory + OsUtils.OS_wrapper("worst.plan")));
        } else if (!conf.getBoolean("parallelism_tune", false)) {
            f = new FileInputStream(new File(directory + OsUtils.OS_wrapper("aware.plan")));
        } else {
            f = new FileInputStream(new File(directory + OsUtils.OS_wrapper("opt.plan")));
        }


// 		ObjectInputStream oi = new ObjectInputStream(f);
//

        final SchedulingPlan plan = SerializationUtils.deserialize(f);//oi.readObject();
//		plan.LOG = LoggerFactory.getLogger(SchedulingPlan.class);
//		oi.close();
        f.close();
        return plan;
    }

    private void ini_cacheMap() {
        cacheMap = new HashMap<>();
        //initialize cache
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            HashMap<String, cache> map = new HashMap<>();
            if (e.operator.input_streams == null) {
                map.put(Constants.DEFAULT_STREAM_ID, new cache());
            } else {
                Set<String> s = new HashSet<>(e.operator.input_streams);//remove duplicate input streams.
                for (String stream : s) {
                    map.put(stream, new cache());
                }
            }
            cacheMap.put(e.getExecutorID(), map);
        }
    }

    private void copyCacheMap(SchedulingPlan currentPlan) {
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            if (e.operator.input_streams == null) {
                cache cache = currentPlan.cacheMap.get(e.getExecutorID()).get(Constants.DEFAULT_STREAM_ID);
                cacheMap.putIfAbsent(e.getExecutorID(), new HashMap<>());
                cacheMap.get(e.getExecutorID()).put(Constants.DEFAULT_STREAM_ID, cache);
            } else {
                Set<String> s = new HashSet<>(e.operator.input_streams);//remove duplicate input streams.
                for (String stream : s)//for each input stream
                {
                    cache cache = currentPlan.cacheMap.get(e.getExecutorID()).get(stream);
                    cacheMap.putIfAbsent(e.getExecutorID(), new HashMap<>());
                    cacheMap.get(e.getExecutorID()).put(stream, cache);
                }
            }
        }
    }

    private String cache_display(ExecutionNode e, String streamId, String defaultStreamId) {

        return String.format("%-15s%-15s%-20s%-20s%-20s%-20s"
                , String.format("\t%.4f", cacheMap.get(e.getExecutorID()).get(streamId).getInputRate() * 1E+6)
                , String.format("\t%.4f", cacheMap.get(e.getExecutorID()).get(streamId).getOutputRate(defaultStreamId) * 1E+6)
                , String.format("\t%.4f", cacheMap.get(e.getExecutorID()).get(streamId).getBounded_processRate() * 1E+6)
                , String.format("\t%.4f", cacheMap.get(e.getExecutorID()).get(streamId).getExpected_processRate() * 1E+6)
                , String.format("\t%.4f", cacheMap.get(e.getExecutorID()).get(streamId).getCycles() / cons.Available_CPU(this.allocation_decision(e)) * 100)
                , String.format("\t%.4f", cacheMap.get(e.getExecutorID()).get(streamId).getMemory() / cons.Available_bandwidth(this.allocation_decision(e)) * 100)
        );
    }

    /**
     * produce plan recodes ready to be loaded and re-use.
     */
    private void plan_to_load() {
        try {
            String directory = System_Plan_Path + OsUtils.OS_wrapper("sesame")
                    + OsUtils.OS_wrapper(conf.getConfigPrefix())
                    + OsUtils.OS_wrapper(String.valueOf(numNodes));
            File file = new File(directory);
            if (!file.mkdirs()) {
            }
            for (TopologyComponent topo : this.graph.topology.getRecords().values()) {
                file = new File(directory + OsUtils.OS_wrapper(topo.getId() + ".txt"));

                try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(file), StandardCharsets.UTF_8))) {

                    writer.write("Parallelism\t" + topo.getNumTasks() + "\n");
                    for (ExecutionNode e : topo.getExecutorList()) {
                        writer.write(e.getExecutorID() + "\t" + mapping.get(e.getExecutorID()) + "\n");
                    }

                    writer.flush();
                    writer.close();
                }
            }
            file = new File(directory
                    + OsUtils.OS_wrapper("stat"));
            Writer writer = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file), StandardCharsets.UTF_8));
            writer.write("output_rate\t" + outputrate + "\n");
            writer.flush();
            writer.close();

        } catch (IOException e) {
            LOG.error("Not able to store the statistics");
        }
    }

    private void serializeMyself() throws IOException {
        String directory = System_Plan_Path + OsUtils.OS_wrapper("sesame")
                + OsUtils.OS_wrapper(conf.getConfigPrefix())
                + OsUtils.OS_wrapper(String.valueOf(numNodes))
                + OsUtils.OS_wrapper(String.valueOf(conf.getDouble("gc_factor", 3)));
        File file = new File(directory);
        if (!file.mkdirs()) {
        }


        FileOutputStream f;
        if (conf.getBoolean("random", false)) {
            f = new FileOutputStream(new File(directory + OsUtils.OS_wrapper("random.plan")));
        } else if (conf.getBoolean("toff", false)) {
            f = new FileOutputStream(new File(directory + OsUtils.OS_wrapper("ff.plan")));
        } else if (conf.getBoolean("roundrobin", false)) {
            f = new FileOutputStream(new File(directory + OsUtils.OS_wrapper("rr.plan")));
        } else if (conf.getBoolean("worst", false)) {
            f = new FileOutputStream(new File(directory + OsUtils.OS_wrapper("worst.plan")));
        } else if (!conf.getBoolean("parallelism_tune", false)) {
            f = new FileOutputStream(new File(directory + OsUtils.OS_wrapper("aware.plan")));
        } else {
            f = new FileOutputStream(new File(directory + OsUtils.OS_wrapper("opt.plan")));
        }

        byte[] serialize = SerializationUtils.serialize(this);
//		ObjectOutputStream o = new ObjectOutputStream(f);
//		o.writeObject(this);
//		o.close();
        f.write(serialize);
        f.close();


        FileWriter ff;
        if (conf.getBoolean("random", false)) {
            ff = new FileWriter(new File(directory + OsUtils.OS_wrapper("random.plan.readable")));
        } else if (conf.getBoolean("toff", false)) {
            ff = new FileWriter(new File(directory + OsUtils.OS_wrapper("ff.plan.readable")));
        } else if (conf.getBoolean("roundrobin", false)) {
            ff = new FileWriter(new File(directory + OsUtils.OS_wrapper("rr.plan.readable")));
        } else if (conf.getBoolean("worst", false)) {
            ff = new FileWriter(new File(directory + OsUtils.OS_wrapper("worst.plan.readable")));
        } else if (!conf.getBoolean("parallelism_tune", false)) {
            ff = new FileWriter(new File(directory + OsUtils.OS_wrapper("aware.plan.readable")));
        } else {
            ff = new FileWriter(new File(directory + OsUtils.OS_wrapper("opt.plan.readable")));
        }

        Writer w = new BufferedWriter(ff);

        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            if (e.operator.input_streams == null) {
                w.write(String.format("%-18s%-10s%s", "\t" + e.getOP(), "\t"
                        + mapping.get(e.getExecutorID()), cache_display(e, Constants.DEFAULT_STREAM_ID, Constants.DEFAULT_STREAM_ID)) + "\n");
            } else {
                Set<String> s = new HashSet<>(e.operator.input_streams);//remove duplicate input streams.
                Set<String> os = new HashSet<>(e.operator.getOutput_streamsIds());//remove duplicate input streams.
                for (String streamId : s) {
                    for (String ostreamId : os) {
                        w.write(String.format("%-18s%-10s%s", "\t" + e.getOP() + "," + e.compressRatio + "," + streamId + ")", "\t"
                                + mapping.get(e.getExecutorID()), cache_display(e, streamId, ostreamId)) + "\n");
                    }
                }
            }
        }
        w.write("======Constraints information=====");
        w.write("\n" + cons.relax_display() + "\n");
        w.write(cons.show(this));
        w.close();
        f.close();
    }

    /**
     * Dump the allocation plan to disk.
     */
    public void planToFile(Configuration conf, boolean BP) {
        if (!conf.getBoolean("monte", false)) {
            planToString(conf.getBoolean("monte", false), BP);
        }
        try {
            serializeMyself();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void ini_allocationMap() {
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            allocationMap.put(e.getExecutorID(), false);
        }
    }

    private void setALLAllocated() {
        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            setAllocated(e);
        }
    }


    public boolean Allocated(ExecutionNode e) {
        assert allocationMap.get(e.getExecutorID()) != null;
        return allocationMap.get(e.getExecutorID());
    }

    public void setAllocated(ExecutionNode e) {
        allocationMap.put(e.getExecutorID(), true);
    }

    public void setDeallocated(ExecutionNode e) {
        allocationMap.put(e.getExecutorID(), false);
    }

    public cache getCacheMap(ExecutionNode e, String streamId) {
        try {
            return cacheMap.get(e.getExecutorID()).get(streamId);
        } catch (Exception ex) {
            LOG.info("Failed to obtain cache!" + e.toString() + streamId);
            return null;
        }
    }

//    public cache getCacheMap(ExecutionNode node) {
//        try {
//        return cacheMap.GetAndUpdate(Constants.DEFAULT_STREAM_ID).GetAndUpdate(node.getExecutorID());
//        } catch (Exception ex) {
//            System.nanoTime();
//            return null;
//        }
//    }

    private void store(ExecutionNode e, String streamId) {
        cacheMap.get(e.getExecutorID()).get(streamId).store();
    }

    public void store(ExecutionNode e) {
        store(e, Constants.DEFAULT_STREAM_ID);
    }

    private void clean(ExecutionNode e, String streamId) {
        cacheMap.get(e.getExecutorID()).get(streamId).clean();
    }

    public void clean(ExecutionNode e) {
        clean(e, Constants.DEFAULT_STREAM_ID);
    }

    private void restore(ExecutionNode e, String streamId) {
        cacheMap.get(e.getExecutorID()).get(streamId).restore();
    }

    public void restore(ExecutionNode e) {
        restore(e, Constants.DEFAULT_STREAM_ID);
    }


    private boolean alert(ExecutionNode e, String streamId) {
        return cacheMap.get(e.getExecutorID()).get(streamId).alert();
    }

    public boolean alert(ExecutionNode e) {
        return alert(e, Constants.DEFAULT_STREAM_ID);
    }

    public void cache_clean() {
        ini_cacheMap();
        outputrate = -1;
    }

    @Override
    public int compareTo(SchedulingPlan o) {
        for (int i : mapping.keySet()) {
            if (!this.mapping.get(i).equals(o.mapping.get(i))) {
                return -1;
            }
        }
        return 0;
    }

    public SchedulingPlan AllLocal(ExecutionGraph graph) {
        int spout_socket = 0;
        for (ExecutionNode executionNode : graph.getExecutionNodeArrayList()) {
//            if (executionNode.operator.type == spoutType)
//                allocate(executionNode, 1);
//                if (spout_socket > numNodes) {
//                    LOG.info("Insufficient num_socket to allocate spout.");
//                    System.exit(-1);
//                }
//            } else
            allocate(executionNode, 0);
        }
        return this;
    }

    /**
     * TODO: think about how to implement the ``worst plan"
     *
     * @param graph
     * @return
     */
    SchedulingPlan AllRemote(ExecutionGraph graph) {
        int i = 0;
        for (ExecutionNode executionNode : graph.getExecutionNodeArrayList()) {
            allocate(executionNode, 0);
        }
        return this;
    }

    public boolean is_collocation(int srcId, int dstId) {
        return Allocated(graph.getExecutionNodeArrayList().get(srcId)) && Allocated(graph.getExecutionNodeArrayList().get(dstId)) && mapping.get(srcId).equals(mapping.get(dstId));
    }


    public void buildFromFileForBenchmark(int i, String prefix) throws FileNotFoundException {
        String path = MAP_Path + OsUtils.OS_wrapper("resourceBenchmark") + OsUtils.OS_wrapper(prefix);
        File file = new File(path);
        if (!file.mkdirs()) {
            LOG.error("Not able to create parent directories");
        }
        Scanner sc = new Scanner(new File(path.concat(OsUtils.OS_wrapper("benchmark")).concat(String.valueOf(i))));
        LOG.info(sc.nextLine());
        while (sc.hasNext()) {
            core_mapping.add(sc.nextInt());
        }
    }

    /**
     * The value_list (objective function) of every
     * intermediate solution is obtained by evaluating the objective
     * function by fixing the allocation of valid operators while
     * assuming the requirements of remaining operators being
     * satisfied as much as possible.
     *
     * @return
     */
    public double getBound_rate() {
        GraphMetrics gM = new GraphMetrics(this, conf.getBoolean("backPressure", false));//clean_executorInformation cache here.
        //TODO: this does not guarantee strictly ``upper" bond. Consider adding a flag in operator to indicate the valid status
        if (this.success()) {
            return getOutput_rate(true);
        } else {
            return gM.getOutput_rate(true);
        }
    }

    public double getOutput_rate(boolean BP) {
        if (!allocated) {
            return 0;
        }

        GraphMetrics gM;

        if (BP) {
            gM = new GraphMetrics(this, true);
        } else {
            gM = new GraphMetrics(this, false);
        }

        //At this moment, cacheMap is correct.
        this.outputrate = gM.getOutput_rate(false);
        return this.outputrate;
    }


    public void finilize() {
        for (ExecutionNode executionNode : graph.getExecutionNodeArrayList()) {
            if (executionNode.operator.input_streams == null)//SPOUT
            {
                cache cache = cacheMap.get(executionNode.getExecutorID()).get(Constants.DEFAULT_STREAM_ID);
                cache.setInputRate(executionNode.getInputRate(Constants.DEFAULT_STREAM_ID, this, false));

                cache.setBounded_processRate(executionNode.getBoundedProcessRate(Constants.DEFAULT_STREAM_ID, this, false));
                cache.setExpected_processRate(executionNode.getExpectedProcessRate(Constants.DEFAULT_STREAM_ID, this, false));
                cache.setOutputRate(executionNode.getOutputRate(Constants.DEFAULT_STREAM_ID, Constants.DEFAULT_STREAM_ID, this, false));
                cache.setCycles(executionNode.getdemandCycles(Constants.DEFAULT_STREAM_ID, this, false));
                cache.setMemory(executionNode.getMemConsumption(this, false));
            } else {
                Set<String> s = new HashSet<>(executionNode.operator.input_streams);//remove duplicate input streams.
                for (String streamId : s) {
                    cache cache = cacheMap.get(executionNode.getExecutorID()).get(streamId);
                    cache.setInputRate(executionNode.getInputRate(streamId, this, false));

                    cache.setBounded_processRate(executionNode.getBoundedProcessRate(streamId, this, false));
                    cache.setExpected_processRate(executionNode.getExpectedProcessRate(streamId, this, false));
                    double sum = 0;
                    for (String ostreamId : executionNode.operator.getOutput_streamsIds()) {
                        sum += executionNode.getOutputRate(streamId, ostreamId, this, false);
                    }
                    cache.setOutputRate(sum);
                    cache.setCycles(executionNode.getdemandCycles(streamId, this, false));
                    cache.setMemory(executionNode.getMemConsumption(streamId, this, false));
                }
            }
        }

    }

    public void cache_finilize_woBP(String streamId) {
        finilize();
    }

    public void planToString(boolean monte, boolean BP) {
        if (!this.success()) {
            LOG.info("==========(Bounded) Plan to String============");
        } else {

            if (BP) {
                LOG.info("==========Plan to String============");
            } else {
                LOG.info("==========Plan to String (without BP) ============");
            }
            this.cache_clean();
            this.getOutput_rate(BP);
        }

        LOG.info(String.format("%-18s%-10s%-15s%-15s%-15s%-15s%-15s%-15s", "\t" + "BasicBoltBatchExecutor", "\tMapping", "\tInput"
                , "\tOutput", "\tProcess(bounded)", "\tProcess(expected)", "\tCycles(%)", "\tMemory(%)"));

        for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
            if (e.operator.input_streams == null) {
                LOG.info(String.format("%-18s%-10s%s", "\t" + e.getOP_full(), "\t"
                        + mapping.get(e.getExecutorID()), cache_display(e, Constants.DEFAULT_STREAM_ID, Constants.DEFAULT_STREAM_ID)));
            } else {
                Set<String> s = new HashSet<>(e.operator.input_streams);//remove duplicate input streams.
                for (String streamId : s) {
                    LOG.info(String.format("%-18s%-10s%s", "\t" + e.getOP_full() + "," + e.compressRatio + "," + streamId + ")", "\t"
                            + mapping.get(e.getExecutorID()), cache_display(e, streamId, Constants.DEFAULT_STREAM_ID)));
                }
            }
        }
        LOG.info("\n" + cons.relax_display() + "\n");
    }


    public void mapFromFile(int i, String prefix) throws FileNotFoundException {
        String path = MAP_Path + OsUtils.OS_wrapper(prefix);
        File file = new File(path);
        if (!file.mkdirs()) {
            LOG.error("Not able to create parent directories");
        }
        Scanner sc = new Scanner(new File(path.concat(OsUtils.OS_wrapper("mapping")).concat(String.valueOf(i))));
        LOG.info(sc.nextLine());
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            String[] entry = line.split(" ");
            mapping.put(Integer.parseInt(entry[0]), Integer.parseInt(entry[1]));
        }
    }

    public boolean success() {
        return allocated;
    }

    public void set_failed() {
        allocated = false;
    }

    public void set_success() {
        setALLAllocated();//used in simulation or manual plan.
        allocated = true;
    }

    public int allocation_decision(ExecutionNode k) {
        return allocation_decision(k.getExecutorID());
    }

    public int allocation_decision(int e_id) {
        Integer integer = mapping.get(e_id);
        if (integer == null) {
            mapping.put(e_id, DEFAULT_Socket_ID);
            return DEFAULT_Socket_ID;//if not specified, allocate to default DEFAULT_Socket_ID
        }
        return integer;
    }

    public long[] allowedCores() {
        return core_mapping.stream().mapToLong(i -> i).toArray();
    }

    /**
     * first fit collocation
     *
     * @param validationMap
     * @param dec
     */
    public SchedulingPlan valid_collocation(Map<Integer, Boolean> validationMap, Decision dec) {
        assert validationMap != null;
        deallocate(validationMap, dec.producer);
        deallocate(validationMap, dec.consumer);
        int satisfy;//by default it is satisfied.
        for (int i = 0; i < numNodes; i++) {
            allocate(validationMap, dec.producer, i);
            allocate(validationMap, dec.consumer, i);
            satisfy = cons.valid_satisfy(this);
            if (satisfy != cons.allstatisfy) {
                deallocate(validationMap, dec.producer);
                deallocate(validationMap, dec.consumer);
            } else {
                validOperators += 2;//valid_collocation

//				final boolean measure_end = cons.valid_check(this);
//				if (!measure_end) {
//					LOG.info("The plan returned from BnB is wrong!");
//					LOG.info(cons.show(this));
//					System.exit(-1);
//				}

                return this;//TODO: first fit by now. Change to best fit later.
            }
        }
        return null;
    }

    /**
     * Try to allocate an executor
     *
     * @param node
     * @param socket
     * @return
     */
    public SchedulingPlan valid_allocate(ExecutionNode node, int socket) {
        //deallocate(validationMap, node);
        int satisfy;//by default it is satisfied.
        assert validationMap != null;
        allocate(validationMap, node, socket);
        satisfy = cons.valid_satisfy(this);
        if (satisfy != cons.allstatisfy) {
            deallocate(validationMap, node);
            return null;
        }

//		final boolean measure_end = cons.valid_check(this);
//		if (!measure_end) {
//			LOG.info("The plan returned from BnB is wrong!");
//			LOG.info(cons.show(this));
//			System.exit(-1);
//		}

        validOperators++;//valid_allocate
        return this;

    }

    /**
     * Reverse allocate a partition executor
     *
     * @param node
     * @return
     */
    public SchedulingPlan valid_deallocate(ExecutionNode node) {
        //deallocate(validationMap, node);
        int satisfy;//by default it is satisfied.
        assert validationMap != null;
        deallocate(validationMap, node);
        validOperators--;//valid_allocate
        return this;

    }


    public void allocate(int eid, int i) {
        allocate(null, eid, i);
    }

    public void allocate(ExecutionNode executor, int i) {
        allocate(null, executor, i);
    }

    public void deallocate(ExecutionNode executor) {
        deallocate(null, executor);
    }

    public void deallocate(int executorID) {
        deallocate(null, executorID);
    }


    private void allocate(Map<Integer, Boolean> validationMap, int eid, int socket) {
        mapping.put(eid, socket);
        allocationMap.put(eid, true);
        if (validationMap != null) {
            validationMap.put(eid, true);
        }
    }

    private void allocate(Map<Integer, Boolean> validationMap, ExecutionNode executor, int socket) {
        allocate(validationMap, executor.getExecutorID(), socket);
    }

    private void deallocate(Map<Integer, Boolean> validationMap, int executorID) {
        if (mapping.containsKey(executorID)) {
            mapping.remove(executorID);
            allocationMap.put(executorID, false);
            if (validationMap != null) {
                validationMap.put(executorID, false);
            }
        }
    }

    private void deallocate(Map<Integer, Boolean> validationMap, ExecutionNode executor) {
        deallocate(validationMap, executor.getExecutorID());
    }

}
