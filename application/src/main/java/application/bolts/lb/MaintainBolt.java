package application.bolts.lb;

import application.constants.VoterSStoreExampleConstants;
import application.util.OsUtils;
import application.util.datatypes.StreamValues;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.OutputFieldsDeclarer;
import sesame.execution.runtime.tuple.impl.Tuple;
import state_engine.DatabaseException;
import state_engine.SimpleDatabase;
import state_engine.query.QueryPlan;
import state_engine.query.QueryPlanException;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.IntDataBox;
import state_engine.storage.datatype.TimestampType;
import state_engine.storage.table.RowID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static application.constants.VoterSStoreExampleConstants.Stream.trendingLeaderboard;


public class MaintainBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MaintainBolt.class);
    private static final long serialVersionUID = -3864274641796701706L;

    public MaintainBolt() {
        super(LOG);
    }


    /**
     * "SELECT cnt FROM votes_count WHERE row_id = 1;"
     */
    private int checkNumVotesStmt(SimpleDatabase.Transaction transaction) {

        try {
            QueryPlan queryPlan = transaction.query("votes_count");
            queryPlan.select("row_id", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(1));
            List<String> columnNames = new ArrayList<>();
            columnNames.add("cnt");
            queryPlan.project(columnNames);
            final Iterator<SchemaRecord> output = queryPlan.execute();

            // validate the maximum limit for votes number
//			if (validation[0].getRowCount() == 0) {
//				return VoterConstants.ERR_INVALID_CONTESTANT;
//			}
//			transaction.end();

            if (output.hasNext()) {
                return output.next().getValues().get(0).getInt();
            }

        } catch (DatabaseException | QueryPlanException e) {
            e.printStackTrace();
        }
        return 0;
    }


    /**
     * "INSERT INTO trending_leaderboard (vote_id, phone_number, state, contestant_number, created, ts) VALUES (?,?,?,?,?,?);"
     *
     * @param transaction
     * @param voteId
     * @param phoneNumber
     * @param state
     * @param contestantNumber
     * @param created
     * @param ts
     */
    private void trendingLeaderboardStmt(SimpleDatabase.Transaction transaction
            , Long voteId, Long phoneNumber, String state, int contestantNumber, TimestampType created, long ts) {

        // put into the window
        StreamValues value = new StreamValues(voteId, phoneNumber, state, contestantNumber, created, ts);
//		collector.emit(trendingLeaderboard, bid, value_list);

    }


    @Override
    public void execute(Tuple input) {
//		SimpleDatabase.Transaction transaction = db.beginTransaction();

//return new Fields(Field.voteId, Field.phoneNumber, Field.state, Field.contestantNumber, Field.timestamp, Field.ts);
        final Long voteId = input.getLong(0);
        final Long phoneNumber = input.getLong(1);
        final String state = input.getString(2);
        int contestantNumber = input.getInt(3);
        TimestampType created = input.getTimestampType(4);
        long ts = input.getLong(5);

        //retrieve the latest vote from the input stream
//		public final SQLStmt getInStreamStmt = new SQLStmt(
//				"SELECT vote_id, phone_number, state, contestant_number, created, ts FROM proc_one_out ORDER BY vote_id LIMIT 1;"
//		);

        //	checkNumVotesStmt
        //	int numVotes = (int)(validation[1].fetchRow(0).getLong(0)) + 1;
//		final int numVotes = checkNumVotesStmt( ) + 1;
//
//		//trendingLeaderboardStmt
//
//		trendingLeaderboardStmt( , voteId, phoneNumber, state, contestantNumber, created, ts);
//
//		//updateNumVotesStmt
//		try {
//			updateNumVotesStmt( , (numVotes % VoterSStoreExampleConstants.VOTE_THRESHOLD));
//		} catch (DatabaseException e) {
//			e.printStackTrace();
//		}

    }

    /**
     * "UPDATE votes_count SET cnt = ? WHERE row_id = 1;"
     *
     * @param transaction
     * @param cnt
     */
    private void updateNumVotesStmt(SimpleDatabase.Transaction transaction, int cnt) throws DatabaseException {
        transaction.updateRecord("votes_count", new LinkedList<>(Collections.singletonList(new IntDataBox(1))), new RowID(1));
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID();
        LOG.info("PID  = " + pid);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream(trendingLeaderboard, new Fields(VoterSStoreExampleConstants.Field.voteId, VoterSStoreExampleConstants.Field.phoneNumber,
                VoterSStoreExampleConstants.Field.state, VoterSStoreExampleConstants.Field.contestantNumber,
                VoterSStoreExampleConstants.Field.timestamp, VoterSStoreExampleConstants.Field.ts));
    }

}
