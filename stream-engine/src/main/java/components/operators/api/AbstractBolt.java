package components.operators.api;

import db.DatabaseException;
import execution.runtime.tuple.JumboTuple;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

/**
 * Created by shuhaozhang on 19/9/16.
 */
public abstract class AbstractBolt extends Operator {
    private static final long serialVersionUID = 7108855719083101853L;

    AbstractBolt(Logger log, Map<String, Double> input_selectivity, Map<String, Double> output_selectivity, double branch_selectivity
            , double read_selectivity, double w) {
        super(log, input_selectivity, output_selectivity, branch_selectivity, read_selectivity, w);
    }

    AbstractBolt(Logger log, Map<String, Double> input_selectivity,
                 Map<String, Double> output_selectivity, double w) {
        super(log, input_selectivity, output_selectivity, 1, 1, w);
    }

    public abstract void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException, IOException;

    public void execute(JumboTuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            execute(new Tuple(in.getBID(), in.getSourceTask(), in.getContext(), in.msg[i]));
        }
    }
}
