package application.bolts.lb;

import application.constants.VoterSStoreExampleConstants;
import application.util.OsUtils;
import sesame.components.operators.api.BaseWindowedBolt;
import sesame.components.windowing.TupleWindow;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.constants.VoterSStoreExampleConstants.Stream.trendingLeaderboard;

public class trendingLeaderboardBolt extends BaseWindowedBolt {
    private static final Logger LOG = LoggerFactory.getLogger(trendingLeaderboardBolt.class);
    private static final long serialVersionUID = -8132374441495286495L;

    public trendingLeaderboardBolt(double window_size) {
        super(window_size);
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
     * --> this essentially means once it receives a tuple as count-based window is used.
     * If this trigger were on a stream, it would fire with every tuple put.
     *
     * @param in
     */
    @Override
    public void execute(TupleWindow in) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(trendingLeaderboard, new Fields(VoterSStoreExampleConstants.Field.voteId, VoterSStoreExampleConstants.Field.phoneNumber,
                VoterSStoreExampleConstants.Field.state, VoterSStoreExampleConstants.Field.contestantNumber,
                VoterSStoreExampleConstants.Field.timestamp, VoterSStoreExampleConstants.Field.ts));
    }
}
