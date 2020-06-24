package application.bolts.lb;

import application.util.OsUtils;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.JumboTuple;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.lang.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class trendingLeaderboard_triggerBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(trendingLeaderboard_triggerBolt.class);
    private static final long serialVersionUID = -7032875354302104239L;
    private final HashMap<Long, MutableInt> counts = new HashMap<>();//contestantNumber, count

    public trendingLeaderboard_triggerBolt() {
        super(LOG);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID();
        LOG.info("PID  = " + pid);

    }

    @Override
    public void cleanup() {

    }

    /**
     * Because this trigger is on a window, it fires every time the window slides.
     * --> this essentially means once it receives a tuple since count-based window is used.
     * If this trigger were on a stream, it would fire with every tuple put.
     * <p>
     * <p>
     * Tuple:
     * VoterSStoreExampleConstants.Field.voteId, VoterSStoreExampleConstants.Field.phoneNumber,
     * VoterSStoreExampleConstants.Field.state, VoterSStoreExampleConstants.Field.contestantNumber,
     * VoterSStoreExampleConstants.Field.timestamp, VoterSStoreExampleConstants.Field.ts
     *
     * @param in
     */
    @Override
    public void execute(Tuple in) throws InterruptedException {
        final Long contestantNumber = in.getLong(3);
        final long bid = in.getBID();
//		if (in.isTickerMark()) {
//			//contestantNumber,
//			collector.emit(bid, new StreamValues(contestantNumber, counts.GetAndUpdate(contestantNumber)));
//		} else {
//			MutableInt count = counts.computeIfAbsent(contestantNumber, k -> new MutableInt(0));
//			count.increment();
//		}
    }

    @Override
    public void execute(JumboTuple in) throws InterruptedException {
        LOG.info("Not supported yet.");
    }
}
