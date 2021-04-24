package components.operators.api;
import org.slf4j.Logger;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import state_engine.db.DatabaseException;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
/**
 * Created by shuhaozhang on 19/9/16.
 */
public abstract class AbstractBolt extends Operator {
    private static final long serialVersionUID = 7108855719083101853L;
    private AbstractBolt(Logger log, boolean byP, double event_frequency, double w) {
        super(log, byP, event_frequency, w);
    }
    AbstractBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double branch_selectivity
            , double read_selectivity, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, false, event_frequency, w);
    }
    AbstractBolt(Logger log, Map<String, Double> input_selectivity,
                 Map<String, Double> output_selectivity, boolean byP, double event_frequency, double w) {
        super(log, input_selectivity, output_selectivity, 1, 1, byP, event_frequency, w);
    }
    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException;
    public void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            execute(new Tuple(in.getBID(), in.getSourceTask(), in.getContext(), in.msg[i]));
        }
    }
    @Override
    public void cleanup() {
        state = null;
    }
    /**
     * When all my consumers callback_bolt, I force synchronize
     *
     * @param callee
     * @param marker
     */
    public void callback(int callee, Marker marker) {
        state.callback_bolt(callee, marker, executor);
    }
    /**
     * used for ordered execution.
     *
     * @param in
     */
    public void _execute(Tuple in) {
    }
    /**
     * used for ordered execution.
     *
     * @param in
     */
    public void _execute(JumboTuple in) {
    }
    /**
     * not sure if this is still required if we force emit one tuple by one tuple.
     *
     * @param gap
     */
    protected void clean_gap(LinkedList<Long> gap) {
//		for (int i = 0; i < gap.size(); i++) {
//			Long g = gap.remove(i);
//			if (this.collector.getBID(POSITION_REPORTS_STREAM_ID) <= g) {
//				gap.add(i, g);
//				return;
//			}
//		}
    }
    public void profile_execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        execute(in);
    }
}
