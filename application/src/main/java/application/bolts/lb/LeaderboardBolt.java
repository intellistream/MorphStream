package application.bolts.lb;

import application.constants.VoterSStoreExampleConstants;
import application.util.OsUtils;
import sesame.components.context.TopologyContext;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import state_engine.DatabaseException;
import state_engine.SimpleDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static application.constants.VoterSStoreExampleConstants.Stream.trendingLeaderboard;

public class LeaderboardBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderboardBolt.class);
    private static final long serialVersionUID = 1060013808735328649L;


    public LeaderboardBolt() {
        super(LOG);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        LOG.info("PID  = " + pid);

    }

    @Override
    public void cleanup() {

    }

    /**
     * Because this trigger is on a window, it fires every time the window slides.
     * --> this essentially means once it receives a tuple since count-based window is used.
     * If this trigger were on a stream, it would fire with every tuple put.
     *
     * @param in
     */
    @Override
    public void execute(Tuple in) throws InterruptedException {
//		SimpleDatabase.Transaction transaction = db.beginTransaction();
//		LeaderboardTrigger.deleteLeaderboard(transaction);
//		LeaderboardTrigger.updateLeaderboard(transaction, in);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(trendingLeaderboard, new Fields(VoterSStoreExampleConstants.Field.voteId, VoterSStoreExampleConstants.Field.phoneNumber,
                VoterSStoreExampleConstants.Field.state, VoterSStoreExampleConstants.Field.contestantNumber,
                VoterSStoreExampleConstants.Field.timestamp, VoterSStoreExampleConstants.Field.ts));
    }

    /**
     * These triggers execute the attached SQL code immediately upon the insertion of a tuple.
     * Note that if a batch of many tuples is inserted with one command, the trigger will fire once for each insertion.
     * <p>
     * <p>
     * All statements will run sequentially.
     */
    static class LeaderboardTrigger {


        /**
         * "DELETE FROM leaderboard;" Essentially, relax_reset the leaderboard here.
         *
         * @param transaction
         */
        static void deleteLeaderboard(SimpleDatabase.Transaction transaction) {
            try {
                transaction.deleteRecord("leaderboard");
            } catch (DatabaseException e) {
                e.printStackTrace();
            }
        }

        /**
         * INSERT INTO leaderboard (contestant_number, num_votes)
         * SELECT trending_leaderboard.contestant_number, count(*) FROM trending_leaderboard, contestants
         * WHERE trending_leaderboard.contestant_number = contestants.contestant_number GROUP BY trending_leaderboard.contestant_number;"
         *
         * @param transaction
         * @param input
         */
        static void updateLeaderboard(SimpleDatabase.Transaction transaction, Tuple input) {

            //				VoterSStoreExampleConstants.Field.voteId, VoterSStoreExampleConstants.Field.phoneNumber,
            //				VoterSStoreExampleConstants.Field.state, VoterSStoreExampleConstants.Field.contestantNumber,
            //				VoterSStoreExampleConstants.Field.timestamp, VoterSStoreExampleConstants.Field.ts


        }
    }

}
