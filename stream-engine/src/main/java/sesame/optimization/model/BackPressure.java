package sesame.optimization.model;

import application.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.TopologyComponent;
import sesame.execution.ExecutionGraph;
import sesame.execution.ExecutionNode;
import sesame.optimization.impl.SchedulingPlan;

import java.util.*;

/**
 * Created by tony on 7/5/2017.
 */
public class BackPressure {
    private final static Logger LOG = LoggerFactory.getLogger(BackPressure.class);

    public static boolean BP(final SchedulingPlan sp) {
        boolean reduced = false;
        double original_input_rate = sp.variables.SOURCE_RATE;
        final double pressureCorrection = backPressureCorrection(sp, sp.graph);

        reduced = !(pressureCorrection > original_input_rate);
        sp.variables.SOURCE_RATE = pressureCorrection;
        sp.BP_calculated = true;
        return reduced;
        //sp.cache_clean();
        //sp.finilize();
        // sp.planToString(false);
    }

    private static double max_demandInputRate(ExecutionNode s, ExecutionNode s_, SchedulingPlan sp) {

        // double x;//let x be the max demand input rate of s.
        if (s.isLeafNode()) {//sink
            double target = Double.MAX_VALUE;
//            try {
            Set<String> set_s = new HashSet<>(s.operator.input_streams);//remove duplicate input streams.
            for (String input_streams : set_s) {//if it has multiple input streams, it can only allow the /$slowest$/ one.
                if (s_.operator.getOutput_streams().containsKey(input_streams)) //make sure this stream is from this parentE.
                {
                    target = Math.min(target, s.getBoundedProcessRate(input_streams, s_, sp, false));
                }
            }
//            } catch (Exception e) {
//                LOG.info("");
//            }
            return target;
        } else {
            List<Double> lx = new LinkedList<>();
            Set<String> set_s = new HashSet<>();
            if (s.operator.input_streams == null) {
                set_s.add(Constants.DEFAULT_STREAM_ID);
            } else {
                set_s = new HashSet<>(s.operator.input_streams);//remove duplicate input streams.
            }
            for (String input_streams_s : set_s) {//if it has multiple input streams, it can only allow the /$slowest$/ one.
                if (s.isSourceNode() || s_.operator.getOutput_streams().containsKey(input_streams_s)) //make sure this stream is from this parentE.
                {
                    for (TopologyComponent children : s.getChildren_keySet()) {
                        for (ExecutionNode c : children.getExecutorList()) {
                            Set<String> set = new HashSet<>(c.operator.input_streams);//remove duplicate input streams.
                            for (String input_stream_c : set) {
                                if (s.operator.getOutput_streams().containsKey(input_stream_c)) {//make sure this stream is from this parentE.
                                    double ratio = s.getOutputRate(input_streams_s, input_stream_c, s_, sp, false)
                                            / s.getOutputRate(input_streams_s, input_stream_c, sp, false);
//                                    try {
                                    double tmp = max_demandInputRate(c, s, sp)
                                            / (s.RM.output_selectivity.get(input_stream_c)
                                            * s.getPartition_ratio(input_stream_c, c.getOP(), c.getExecutorID())
                                            * c.RM.input_selectivity.get(input_stream_c));
                                    lx.add(tmp * ratio);//      (s_ == null ? 1 : s_.operator.numTasks)
//                                    } catch (Exception e) {
//                                        LOG.info(e.getMessage());
//                                    }
                                }
                            }
                        }
                    }
                }

            }

            /*
             * x=min(s.getBoundedProcessRate(), \forall x')
             * */
            Collections.sort(lx);

            double target = Double.MAX_VALUE;
            if (s.operator.input_streams != null) {
                for (String input_streams : new HashSet<>(s.operator.input_streams)) {//if it has multiple input streams, it can only allow the /$slowest$/ one.
                    if (s_.operator.getOutput_streams().containsKey(input_streams)) //make sure this stream is from this parentE.
                    {
                        target = Math.min(target, s.getBoundedProcessRate(input_streams, s_, sp, false));
                    }
                }
            } else {
                target = Math.min(target, s.getBoundedProcessRate(Constants.DEFAULT_STREAM_ID, s_, sp, false));
            }
            return Math.min(target, lx.get(0));
        }
    }

    private static double backPressureCorrection(final SchedulingPlan sp, ExecutionGraph graph) {
//        LOG.info("======Backpressure_correction started======");
        double externalInput = Double.MAX_VALUE;
        for (int i = 0; i < 2; i++) {
            externalInput = Double.MAX_VALUE;
            for (ExecutionNode e : graph.getExecutionNodeArrayList()) {
                if ((e.operator.isLeadNode())) {
                    double x = max_demandInputRate(e, null, sp);
                    externalInput = Math.min(externalInput, x);
                }
            }
            sp.variables.SOURCE_RATE = externalInput;
            sp.cache_clean();
            sp.finilize();
        }
        return externalInput;
    }
}
