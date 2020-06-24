package sesame.optimization.model;

import application.Constants;
import application.Platform;
import sesame.components.TopologyComponent;
import sesame.components.operators.executor.IExecutor;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.SchedulingPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Created by tony on 6/15/2017.
 */
public class RateModel implements Serializable {
    private static final long serialVersionUID = -6928164802146927824L;
    private final static Logger LOG = LoggerFactory.getLogger(RateModel.class);
    public static long count = 0;
    public final Map<String, Double> input_selectivity;
    public final Map<String, Double> output_selectivity;
    private final double read_selectivity;
    private final double branch_selectivity;
    private final Platform p;
    private final IExecutor _op;
    private final double contention_overhead = 10;//This is a contention overhead factor that shall be profiled.

    public RateModel(IExecutor op, Platform p) {
        _op = op;
        if (op != null) {
            this.read_selectivity = op.get_read_selectivity();
            this.branch_selectivity = op.get_branch_selectivity();
            this.p = p;
            if (op.get_input_selectivity() == null) {
                this.input_selectivity = new HashMap<>();
                this.input_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
            } else {
                this.input_selectivity = op.get_input_selectivity();
            }

            if (op.get_output_selectivity() == null) {
                this.output_selectivity = new HashMap<>();
                this.output_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
            } else {
                this.output_selectivity = op.get_output_selectivity();
            }

        } else {
            this.read_selectivity = 0;
            this.branch_selectivity = 0;
            this.p = p;

            this.input_selectivity = new HashMap<>();
            this.input_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);

            this.output_selectivity = new HashMap<>();
            this.output_selectivity.put(Constants.DEFAULT_STREAM_ID, 1.0);
        }
    }

    public double CleangetOutput_rate(ExecutionNode executionNode
            , String executionNode_InputStreamId, String executionNode_OutputStreamId, final SchedulingPlan sp, boolean bound) {
        sp.cache_clean();
        double output_rate = ro_c(executionNode, executionNode_InputStreamId, executionNode_OutputStreamId, sp, bound);
        sp.cache_finilize_woBP(executionNode_InputStreamId);
        return output_rate;
    }

    private double C_Pro(STAT stat) {
        return stat.executionNode.RM.branch_selectivity * stat.cycles_PS[0];//Math.max(stat.cycles_PS[0], stat.cycles_PS[1]);
    }

    private double C_content(ExecutionNode sp) {
        return contention_overhead * sp.operator.getNumTasks();//Math.max(stat.cycles_PS[0], stat.cycles_PS[1]);
    }

    private double Acutal_FetchCycles(STAT stat, SchedulingPlan sp, ExecutionNode src, ExecutionNode dst) {

//		return Double.compare(sp.allocation_decision(src), sp.allocation_decision(dst)) != 0 ? (
//				p.CLOCK_RATE//xxx(cycles/ns)
//						* //Math.ceil((stat.tuple_size / p.cache_line + 1)) * p.cache_line -- best case, sequential access.
//						stat.executionNode.RM.read_selectivity * (stat.tuple_size * p.cache_line)  // worst case, random access.
//						//xxx bytes per input tuple
//						/ (p.bandwidth_map[sp.allocation_decision(src)][sp.allocation_decision(dst)] * p.bandwdith_convert))//xxx B/nsec
//				: 0;

//		return Double.compare(sp.allocation_decision(src), sp.allocation_decision(dst)) != 0 ? (
//				p.CLOCK_RATE//xxx(cycles/ns)
//						*
//						stat.executionNode.RM.read_selectivity * Math.ceil((stat.tuple_size / p.cache_line + 1))
//
//						* (p.latency_map[sp.allocation_decision(src)][sp.allocation_decision(dst)]))
//				: 0;

        return
                //RLAS
                Double.compare(sp.allocation_decision(src), sp.allocation_decision(dst)) != 0 ? (
                        p.CLOCK_RATE//xxx(cycles/ns)
                                * stat.executionNode.RM.read_selectivity //--- control randomness..
                                * Math.ceil((stat.tuple_size / p.cache_line))
                                * (p.latency_map[sp.allocation_decision(src)][sp.allocation_decision(dst)])
                ) : 0;

//                RLAS-Max
//                p.CLOCK_RATE//xxx(cycles/ns)
//                        * stat.executionNode.RM.read_selectivity //--- control randomness..
//                        * Math.ceil((stat.tuple_size / p.cache_line))
//                        * (p.latency_map[0][7]);
//
                //RLAS-Min
//                0;
    }

    /**
     * @param stat
     * @return
     */
    private double C_Fetch(STAT stat, SchedulingPlan sp, ExecutionNode src, ExecutionNode dst) {

        double rt;
        if (src == null) {
            rt = stat.cycles_PS_inFetch[0];//Math.max(stat.cycles_PS_inFetch[0], stat.cycles_PS_inFetch[1]);
        } else {

            rt = stat.cycles_PS_inFetch[0]
                    + Acutal_FetchCycles(stat, sp, src, dst);
//					* Math.ceil((stat.executionNode.RM.read_selectivity * stat.tuple_size) / p.cache_line)//xxx access per input tuple
//					* p.latency_map[sp.allocation_decision(src)][sp.allocation_decision(dst)];//xxx ns per access;
//			//LOG.DEBUG(dst.getOP_full() + " acutal_Fetch(stat, sp, src, dst):" + Acutal_FetchCycles(stat, sp, src, dst));
        }
        return rt;
    }

    /**
     * @param stat
     * @param sp    @return
     * @param bound
     */
    private double C_Total(STAT stat, ExecutionNode src, ExecutionNode dst, SchedulingPlan sp, boolean bound) {
        double v;

        if (bound) {
            if (src != null && sp.validationMap.get(src.getExecutorID()) && sp.validationMap.get(dst.getExecutorID())) {
                v =
                        C_Pro(stat)//predict local cycles
                                + (C_Fetch(stat, sp, src, dst))//C_Fetch
                ;
            } else {//bound function
                v =
                        C_Pro(stat)//predict local cycles
                                + (C_Fetch(stat, sp, null, null))//C_Fetch
                ;
            }
        } else {
            v = C_Pro(stat)//predict local cycles
                    + (C_Fetch(stat, sp, src, dst));//C_Fetch
        }

//		if (dst.op.isStateful()) {
//			v += C_gc_overhead(sp, stat.gc_factor);
//		} else {
//			v += C_gc_overhead(sp, stat.gc_factor) / 2;//non-stateful operator has less GC overhead..?
//		}

//		v += C_fetch_overhead(dst);
        return v;
    }


    // It has additional gc overhead when using more than 4 sockets.
    // it seems to be 3 for FD.
    private double C_gc_overhead(SchedulingPlan sp, double gc_factor) {

        if (sp.graph.getExecutionNodeArrayList().size() > 34) {
            return sp.graph.getExecutionNodeArrayList().size() * gc_factor;
        }

        return 0;
    }

    /**
     * Additional overhead when an operator needs to fetch from multiple operators.
     * This is in particular used for LR.
     *
     * @param dst
     * @return
     */
    private double C_fetch_overhead(ExecutionNode dst) {
        int size = dst.getParents_keySet().size();
//		return size > 1 ? size * 10.0 : 0;
        return size * 5;//LR : size* 10.
    }

    /**
     * This overhead only applies to producer, hence leave nodes will never has this overhead.
     * It is ignored currently.
     *
     * @param did
     * @param sp
     * @return
     */
    private double coherencyOverhead(int did, SchedulingPlan sp) {

        if (did >= 0) {
            ExecutionNode consumer = sp.graph.getExecutionNode(did);
            if (consumer != null && consumer.operator != sp.graph.getSink().operator) {
                int num_children = 0;
                int num_remote_children = 0;
                for (TopologyComponent child : consumer.getChildren_keySet()) {
                    for (ExecutionNode e_child : child.getExecutorList()) {
                        if (!e_child.idle(consumer)) {//make sure the child is actually fetching data on this stream...
                            num_children++;
                            if (!sp.is_collocation(did, e_child.getExecutorID())) {
                                num_remote_children++;//take worst case consideration. As during the allocation, e_child may not be allocated.
                            }
                        }
                    }
                }
                return num_remote_children / num_children * 900 + num_remote_children * 60;
            }
        }

//        double outputRate_Producer = producer.getOutputRate(streamId, sp);
//        double process_rate_Consumer = consumer.getExpectedProcessRate(streamId, producer, sp);
//        double P_C = outputRate_Producer / process_rate_Consumer;
//        if (P_C > 0.5) {
//            return (500 * P_C + 1000);
//        } else
//            return (500 * P_C + 100);
        return 0;
    }


    private boolean cacheSet(Object value) {
        return value != null
                && Double.compare((Double) value, -1) != 0;
        //   (Double)value_list!=Double.NEGATIVE_INFINITY;
    }


    //cycles(c)
    public double getdemandCycles_c(ExecutionNode executionNode, String streamId, SchedulingPlan schedulingPlan, boolean bound) {

        cache cacheMap = schedulingPlan.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getCycles())) {
            return cacheMap.getCycles();
        }

        if (executionNode.noParents()) {//no producer operator
            double rt = erp_sc(executionNode, null, streamId, schedulingPlan, bound)
                    * cycles_scPertuple(executionNode, null, streamId, schedulingPlan, bound);
            cacheMap.setCycles(rt);
            return rt;
        }

        final Set<TopologyComponent> parents_set = executionNode.getParents_keySet();//multiple parent
        double sum_cycles = 0;

        for (TopologyComponent parents : parents_set) {
            for (ExecutionNode parentE : parents.getExecutorList()) {
                if (parentE.operator.getOutput_streams().containsKey(streamId)) {//make sure this stream is from this parentE.
                    double v = erp_sc(executionNode, parentE, streamId, schedulingPlan, bound);
                    double c = cycles_scPertuple(executionNode, parentE, streamId, schedulingPlan, bound);
                    sum_cycles += v * c;
                }
            }
        }
        cacheMap.setCycles(sum_cycles);
        return sum_cycles;
    }


    /**
     * xxx cycles per ns
     *
     * @param executionNode
     * @param parentE
     * @param streamId
     * @param plan
     * @return
     */
    public double getCommConsumption(ExecutionNode executionNode, ExecutionNode parentE, String streamId, SchedulingPlan plan, boolean bound) {
        cache cacheMap = plan.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getTrans())) {
            return cacheMap.getTrans();
        }
        double rt;
        rt = erp_sc(executionNode, parentE, streamId, plan, bound)//xxx tuples per ns
                * tranCycles_scPerTuple(executionNode.profiling.get(parentE.getExecutorID()), plan, executionNode, parentE);
        cacheMap.setTrans(rt);
        return rt;
    }

    /**
     * xxx Bytes per ns
     *
     * @param executionNode
     * @param parentE
     * @param streamId
     * @param plan
     * @return
     */
    public double getTransConsumption(ExecutionNode executionNode, ExecutionNode parentE, String streamId, SchedulingPlan plan, boolean bound) {
        cache cacheMap = plan.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getTrans())) {
            return cacheMap.getTrans();
        }
        double rt;
        rt = erp_sc(executionNode, parentE, streamId, plan, bound)//xxx tuples per ns
                * tranBytes_scPerTuple(executionNode.profiling.get(parentE.getExecutorID()), executionNode);
        cacheMap.setTrans(rt);
        return rt;
    }


    public double getMemConsumption(ExecutionNode executionNode, String streamId, SchedulingPlan plan, boolean bound) {
        cache cacheMap = plan.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getMemory())) {
            return cacheMap.getMemory();
        }

        double rt = 0;
        if (executionNode.noParents()) {
            return getMemConsumption(executionNode, null, streamId, plan, bound);
        } else {
            for (TopologyComponent parent : executionNode.getParents_keySet()) {
                for (ExecutionNode pE : parent.getExecutorList()) {
                    if (pE.operator.getOutput_streams().containsKey(streamId))//make sure this stream is from this parentE.
                    {
                        rt += getMemConsumption(executionNode, pE, streamId, plan, bound);
                    }
                }
            }
        }
        cacheMap.setMemory(rt);
        return rt;
    }


    private double getMemConsumption(ExecutionNode executionNode, ExecutionNode parentE, String streamId, SchedulingPlan plan, boolean bound) {
        double rt;

        int sid = -1;
        if (parentE != null) {
            sid = parentE.getExecutorID();
        }
        rt = erp_sc(executionNode, parentE, streamId, plan, bound)
                * MembandwidthConsumptionPerTuple(executionNode.profiling.get(sid));
        return rt;
    }


    public double weighted_sum(final Double[] values, final Double[] weights) {
        double sum = 0.0;
        for (int i = 0; i < values.length; i++) {
            sum += values[i] * weights[i];
        }

        return sum;
    }

    public double sum(final Double[] values) {
        double sum = 0.0;
        for (Double value : values) {
            sum += value;
        }
        return sum;
    }

    //r_i(s,c)
    public double ri_sc(ExecutionNode executionNode, ExecutionNode parentE
            , String executionNode_InputStreamId, SchedulingPlan sp, boolean bound) {

        double results = 0;
        int sid = -1;
        if (parentE != null) {
            sid = parentE.getExecutorID();
        }
        cache cacheMap = sp.getCacheMap(executionNode, executionNode_InputStreamId);
        if (cacheSet(cacheMap.getSub_inputRate().get(sid))) {
            return cacheMap.getSub_inputRate().get(sid) * executionNode.RM.input_selectivity.get(executionNode_InputStreamId);
        }
        if (sid == -1)
        //assert executionNode.RM.input_selectivity == 1;//the input selectivity of spout must always be 1.
        {
            results = sp.variables.SOURCE_RATE;
        } else {
            double total_ro = 0;
            if (parentE.operator.input_streams == null) {
                total_ro += ro_c(parentE, Constants.DEFAULT_STREAM_ID, executionNode_InputStreamId, sp, bound);//output rate of parents.
            } else {
                Set<String> s = new HashSet<>(parentE.operator.input_streams);//remove duplicate input streams.
                for (String PstreamId : s) {
                    total_ro += ro_c(parentE, PstreamId, executionNode_InputStreamId, sp, bound);//output rate of parents.
                }
            }

            results = total_ro * executionNode.RM.input_selectivity.get(executionNode_InputStreamId)
                    * parentE.getPartition_ratio(executionNode_InputStreamId, executionNode.getOP(), executionNode.getExecutorID());

        }
        cacheMap.getSub_inputRate().put(sid, results);
        return results;
    }

    //r_i(c)
    public double ri_c(ExecutionNode executionNode, String streamId, SchedulingPlan sp, boolean bound) {
        cache _ri_c = sp.getCacheMap(executionNode, streamId);
        if (cacheSet(_ri_c.getInputRate())) {
            return _ri_c.getInputRate();
        }

        final Set<TopologyComponent> parents_set = executionNode.getParents_keySet();//multiple parent

        if (parents_set.size() == 0) {//no producer operator
            _ri_c.setInputRate(ri_sc(executionNode, null, streamId, sp, bound));//r_i(s,c)
            return _ri_c.getInputRate();
        } else {
            double sum_inputRate = 0;
            for (TopologyComponent parents : parents_set) {
                for (ExecutionNode parentE : parents.getExecutorList()) {
                    if (parentE.operator.getOutput_streams().containsKey(streamId))//make sure this stream is from this parentE.
                    {
                        sum_inputRate += ri_sc(executionNode, parentE, streamId, sp, bound);//r_i(s,c)
                    }
                }
            }
            _ri_c.setInputRate(sum_inputRate);
            return _ri_c.getInputRate();
        }
    }


    /**
     * @return number of bytes accessed from memory required per tuple (or batch).
     */
    private double MembandwidthConsumptionPerTuple(STAT stat) {
        return (stat.LLC_MISS_PS[0] + stat.LLC_MISS_PS_inFetch[0] + stat.LLC_REF_PS[0] + stat.LLC_REF_PS_inFetch[0]) * p.cache_line;
        //(Math.max(this.LLC_MISS_PS[0], this.LLC_MISS_PS[1]) + Math.max(this.LLC_MISS_PS_inFetch[0], this.LLC_MISS_PS_inFetch[1])) * p.cache_line;
    }

    private double tranCycles_scPerTuple(STAT stat, SchedulingPlan plan, ExecutionNode executionNode, ExecutionNode parentE) {
        return C_Fetch(stat, plan, parentE, executionNode);
    }

    private double tranBytes_scPerTuple(STAT stat, ExecutionNode executionNode) {
        return
                (stat.tuple_size
                        * executionNode.RM.read_selectivity
                        + executionNode.RM.read_selectivity != 0 ? 8 : 0);//xxx Bytes per tuple
    }

    //Formula 7:
    //cycles(s,c): CPU cycles need to process one tuple. It has nothing to do with input rate.
    public double cycles_scPertuple(ExecutionNode executionNode, ExecutionNode parentE
            , String streamId, SchedulingPlan schedulingPlan, boolean bound) {
        double rt;
        if (executionNode.isVirtual()) {
            return 0;
        }
        int sid = -1;
        if (parentE != null) {
            sid = parentE.getExecutorID();
        }
        cache cacheMap = schedulingPlan.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getSub_Cycles().get(sid))) {
            return cacheMap.getSub_Cycles().get(sid);
        }

        rt = C_Total(
                executionNode.profiling.get(sid),
                parentE,
                executionNode,
                schedulingPlan, bound
        );

        cacheMap.getSub_Cycles().put(sid, rt);
        return rt;
    }


    //Formula 9:
    //r_p(s,c) unit process rate
    private double rp_sc(ExecutionNode executionNode, ExecutionNode parentE, String streamId,
                         SchedulingPlan sp, boolean bound) {
        int sid = -1;
        if (parentE != null) {
            sid = parentE.getExecutorID();
        }
        cache cacheMap = sp.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getSub_processRate().get(sid))) {
            return cacheMap.getSub_processRate().get(sid);
        }

        double cycles = cycles_scPertuple(executionNode, parentE, streamId, sp, bound);
        double results = p.CLOCK_RATE * executionNode.compressRatio / cycles;//--formula 9.
        cacheMap.getSub_processRate().put(sid, results);
        return results;
    }

    //Formula 10:
    //r_p(c) does not make much sense. It should not be weighted sum,
    // but it also depends on how many tuples are being processed from each producer.
    private double rp_c(ExecutionNode executionNode, String streamId, SchedulingPlan sp, boolean bound) {
        cache cacheMap = sp.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getProcessRate())) {
            return cacheMap.getProcessRate();
        }
        double results = 0;

        if (executionNode.noParents()) {
            results = rp_sc(executionNode, null, streamId, sp, bound);
        } else {
            final Set<TopologyComponent> parents_set = executionNode.getParents_keySet();//multiple parent
            double num = 0;
            double den = 0;
            for (TopologyComponent parents : parents_set) {
                for (ExecutionNode parentE : parents.getExecutorList()) {
                    if (parentE.operator.getOutput_streams().containsKey(streamId)) {//make sure this stream is from this parentE.
                        final double ri_sc = ri_sc(executionNode, parentE, streamId, sp, bound);
                        num += ri_sc;
                        den += ri_sc
                                / rp_sc(executionNode, parentE, streamId, sp, bound);
                    }
                }
            }
            results = num == 0 ? Double.MAX_VALUE : num / den;//if there is no input, we can't tell the actual process rate accordingly.
        }
        cacheMap.setProcessRate(results);
        return results;
    }

    //Formula 11
    //br_p(s,c) bounded unit process rate
    public double brp_sc(ExecutionNode executionNode, ExecutionNode parentE, String streamId,
                         SchedulingPlan sp, boolean bound) {
        final double ri_sc = ri_sc(executionNode, parentE, streamId, sp, bound);

        return ri_sc == 0 ?
                Double.MAX_VALUE
                : rp_c(executionNode, streamId, sp, bound) * ri_sc / ri_c(executionNode, streamId, sp, bound);
    }

    //Formula 11
    //br_p(s,c) bounded process rate
    public double brp_c(ExecutionNode executionNode, String streamId,
                        SchedulingPlan sp, boolean bound) {
        cache cacheMap = sp.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getBounded_processRate())) {
            return cacheMap.getBounded_processRate();
        }

        if (executionNode.noParents()) {
            return rp_c(executionNode, streamId, sp, bound);
        }
        double sum = 0;
        final Set<TopologyComponent> parents_set = executionNode.getParents_keySet();//multiple parent
        for (TopologyComponent parents : parents_set) {
            for (ExecutionNode parentE : parents.getExecutorList()) {
                if (parentE.operator.getOutput_streams().containsKey(streamId))//make sure this stream is from this parentE.
                {
                    sum += brp_sc(executionNode, parentE, streamId, sp, bound);
                }
            }
        }
        cacheMap.setBounded_processRate(sum);
        return sum;
    }

    //Formula 12
    //r_p(s,c) expected unit process rate
    public double erp_sc(ExecutionNode executionNode, ExecutionNode parentE, String streamId,
                         SchedulingPlan sp, boolean bound) {
//        LOG.info("erp_sc called:" + executionNode.getExecutorID() + Thread.currentThread().getId());
        int sid = parentE == null ? -1 : parentE.getExecutorID();
        cache cacheMap = sp.getCacheMap(executionNode, streamId);
//        try {
        if (cacheSet(cacheMap.getSub_expected_processRate().get(sid))) {
            return cacheMap.getSub_expected_processRate().get(sid);
        }
//        } catch (Exception e) {
//            if (cacheMap == null) {
//                LOG.info("Failed to obtain cacheMap, sid:" + sid + "\tExecutionNode:"
//                        + executionNode.show() + "\tParentE:" + parentE.show() + "streamId:" + streamId);
//                try {
//                    System.in.read();
//                } catch (IOException e1) {
//                    e1.printStackTrace();
//                }
//            }
//
//
//            if (cacheMap.getSub_expected_processRate().GetAndUpdate(sid) == null) {
//                LOG.info("Failed to obtain getSub_expected_processRate, sid:" + sid + "\tExecutionNode:"
//                        + executionNode.show() + "\tParentE:" + parentE.show() + "streamId:" + streamId);
//                try {
//                    System.in.read();
//                } catch (IOException e1) {
//                    e1.printStackTrace();
//                }
//            }
//
//            LOG.info("sid:" + sid + "\tExecutionNode:" + executionNode.show() + "\tParentE:" + parentE.show()
//                    + "streamId:" + streamId);
//            try {
//                System.in.read();
//            } catch (IOException e1) {
//                e1.printStackTrace();
//            }
//        }
        double v = Math.min(brp_sc(executionNode, parentE, streamId, sp, bound)
                , ri_sc(executionNode, parentE, streamId, sp, bound));

        cacheMap.getSub_expected_processRate().put(sid, v);

        return v;
    }

    //Formula x
    //r_p(c) expected overhead_total process rate
    public double erp_c(ExecutionNode executionNode, String streamId,
                        SchedulingPlan sp, boolean bound) {
        cache cacheMap = sp.getCacheMap(executionNode, streamId);
        if (cacheSet(cacheMap.getExpected_processRate())) {
            return cacheMap.getExpected_processRate();
        }

        if (executionNode.noParents()) {
            return erp_sc(executionNode, null, streamId, sp, bound);
        }
        double sum = 0;
        final Set<TopologyComponent> parents_set = executionNode.getParents_keySet();//multiple parent
        for (TopologyComponent parents : parents_set) {
            for (ExecutionNode parentE : parents.getExecutorList()) {
                if (parentE.operator.getOutput_streams().containsKey(streamId))//make sure this stream is from this parentE.
                {
                    sum += erp_sc(executionNode, parentE, streamId, sp, bound);
                }
            }
        }
        cacheMap.setExpected_processRate(sum);
        return sum;
    }


    /**
     * Formula 4 r_o(s,c)
     * Represents how many tuple are generated in O_c due to the handling of tuple from O_s.
     *
     * @param executionNode
     * @param parentE
     * @param executionNode_InputStreamId
     * @param executionNode_OnputStreamId
     * @param sp
     * @param bound
     * @return ro_sc of specific outputStream <>executionNode_OnputStreamId</>..
     */
    public double ro_sc(ExecutionNode executionNode,
                        ExecutionNode parentE,
                        String executionNode_InputStreamId,
                        String executionNode_OnputStreamId,
                        SchedulingPlan sp, boolean bound) {
        int sid = -1;
        if (parentE != null) {
            sid = parentE.getExecutorID();
        }
        double rt = 0;
        cache cacheMap = sp.getCacheMap(executionNode, executionNode_InputStreamId);

        HashMap<String, Double> cached_ro_sc = cacheMap.getSub_outputRate().get(sid);

        if (cached_ro_sc != null && cacheSet(cached_ro_sc.get(executionNode_OnputStreamId))) {
            return cached_ro_sc.get(executionNode_OnputStreamId);
        }


        rt = erp_sc(executionNode, parentE, executionNode_InputStreamId, sp, bound)
                * executionNode.RM.output_selectivity.get(executionNode_OnputStreamId);

        if (cached_ro_sc == null) {
            HashMap<String, Double> map = new HashMap<>();
            map.put(executionNode_OnputStreamId, rt);
            cacheMap.getSub_outputRate().put(sid, map);
        } else {
            cached_ro_sc.put(executionNode_OnputStreamId, rt);
            cacheMap.getSub_outputRate().put(sid, cached_ro_sc);
        }

        return rt;
    }

    //r_o(c) =
    public double ro_c(ExecutionNode executionNode, String executionNode_InputStreamId
            , String executionNode_OnputStreamId, final SchedulingPlan sp, boolean bound) {
        cache _ro_c = sp.getCacheMap(executionNode, executionNode_InputStreamId);
        if (_ro_c != null && cacheSet(_ro_c.getOutputRate(executionNode_OnputStreamId))) {
            return _ro_c.getOutputRate(executionNode_OnputStreamId);
        }
        if (executionNode.noParents()) {//no producer operator
            double rt = ro_sc(executionNode, null, executionNode_InputStreamId, executionNode_OnputStreamId, sp, bound);//r_o(s,c)

            assert _ro_c != null;
            _ro_c.setOutputRate(rt, executionNode_OnputStreamId);

            return rt;
        } else {
            double sum_outputRate = 0;
            final Set<TopologyComponent> parents_set = executionNode.getParents_keySet();//multiple parent
            for (TopologyComponent parents : parents_set) {
                for (ExecutionNode parentE : parents.getExecutorList()) {
                    if (parentE.operator.getOutput_streams().containsKey(executionNode_InputStreamId)) {//make sure this stream is from this parentE.
                        double rt = ro_sc(executionNode, parentE, executionNode_InputStreamId, executionNode_OnputStreamId, sp, bound);//r_o(s,c)
                        sum_outputRate += rt;
                    }
                }
            }
            assert _ro_c != null;
            _ro_c.setOutputRate(sum_outputRate, executionNode_OnputStreamId);
            return sum_outputRate;
        }
    }
}
