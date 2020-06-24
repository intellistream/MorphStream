package application.bolts.lb;

import application.constants.VoterSStoreExampleConstants;
import application.spout.PhoneCallGenerator;
import application.util.OsUtils;
import application.util.datatypes.StreamValues;
import sesame.components.context.TopologyContext;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.impl.Fields;
import sesame.execution.runtime.tuple.impl.Tuple;
import org.apache.commons.lang.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.DatabaseException;
import state_engine.SimpleDatabase;
import state_engine.query.QueryPlan;
import state_engine.query.QueryPlanException;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.*;

import java.util.*;

import static application.constants.VoterSStoreExampleConstants.*;

public class VoteBolt extends MapBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VoteBolt.class);
    private static final long serialVersionUID = -4550688878746812305L;
    //private int total_thread=context.getThisTaskId();
//    private static final String splitregex = " ";
//    private static LinkedList<String> logger = new LinkedList<String>();
    private final Map<String, MutableLong> counts = new HashMap<>();

    //    long start = 0, end = 0, curr = 0;
    int loop = 1;

    public VoteBolt() {
        super(LOG);
        int cnt = 0;
    }


    /**
     * "SELECT contestant_number FROM contestants WHERE contestant_number = ?;"
     *
     * @param transaction
     * @param contestantNumber
     */
    private long checkContestant(SimpleDatabase.Transaction transaction, int contestantNumber) {
//		SimpleDatabase.Transaction transaction = db.beginTransaction();
        try {
            transaction.queryAs("contestants", "c");
            QueryPlan queryPlan = transaction.query("contestants");
            queryPlan.select("c.contestant_number", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(contestantNumber));
            final Iterator<SchemaRecord> output = queryPlan.execute();

            // validate the maximum limit for votes number
//			if (validation[0].getRowCount() == 0) {
//				return VoterConstants.ERR_INVALID_CONTESTANT;
//			}
//			transaction.end();

            if (!output.hasNext()) {
                return ERR_INVALID_CONTESTANT;
            }

        } catch (DatabaseException | QueryPlanException e) {
            e.printStackTrace();
        }
        return VOTE_SUCCESSFUL;
    }


    /**
     * -- rollup of votes by phone number, used to reject excessive voting
     * CREATE VIEW v_votes_by_phone_number
     * (
     * phone_number
     * , num_votes
     * )
     * AS
     * SELECT phone_number
     * , COUNT(*)
     * FROM votes
     * GROUP BY phone_number
     * ;
     */
    private void v_votes_by_phone_number(SimpleDatabase.Transaction transaction, Long phoneNumber) throws DatabaseException, QueryPlanException {
        transaction.createTempTable(transaction.getSchema("votes"), "v_votes");
        QueryPlan queryPlan = transaction.query("votes");
        queryPlan.groupBy("phone_number");
        queryPlan.count();

        List<String> columnNames = new ArrayList<>();
        columnNames.add("phone_number");
        queryPlan.project(columnNames);

        Iterator<SchemaRecord> recordIterator = queryPlan.execute();

        if (recordIterator.hasNext()) {
            SchemaRecord record = recordIterator.next();
            List<DataBox> values = record.getValues();
            transaction.addRecord("v_votes", record);
        }
    }


    /**
     * "SELECT num_votes FROM v_votes_by_phone_number WHERE phone_number = ?;"
     *
     * @param transaction
     * @param phoneNumber
     */
    private long checkVoterStmt(SimpleDatabase.Transaction transaction, Long phoneNumber) {

        try {
            v_votes_by_phone_number(transaction, phoneNumber);
            QueryPlan queryPlan = transaction.query("v_votes");
            queryPlan.select("phone_number", QueryPlan.PredicateOperator.EQUALS, new LongDataBox(phoneNumber));

            Iterator<SchemaRecord> recordIterator = queryPlan.execute();
            if (recordIterator.hasNext()) {
                SchemaRecord record = recordIterator.next();
                List<DataBox> values = record.getValues();//it shall be 0 or 1.
                if (values.get(0).getInt() > VoterSStoreExampleConstants.MAX_VOTES) {
                    return ERR_VOTER_OVER_VOTE_LIMIT;
                }
            }

        } catch (DatabaseException | QueryPlanException e) {
            e.printStackTrace();
        }

        return VOTE_SUCCESSFUL;
    }

    /**
     * "SELECT state FROM area_code_state WHERE area_code = ?;"
     *
     * @param transaction
     * @param area_code
     */
    private String checkStateStmt(SimpleDatabase.Transaction transaction, short area_code) {
//		SimpleDatabase.Transaction transaction = db.beginTransaction();
        try {
            QueryPlan queryPlan = transaction.query("area_code_state");
            queryPlan.select("area_code", QueryPlan.PredicateOperator.EQUALS, new IntDataBox(area_code));
            List<String> columnNames = new ArrayList<>();
            columnNames.add("state");
            queryPlan.project(columnNames);
            final Iterator<SchemaRecord> output = queryPlan.execute();
//			transaction.end();

            // Some sample client libraries use the legacy random phone generation that mostly
            // created invalid phone numbers. Until refactoring, re-assign all such votes to
            // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
            // but are tracked as legitimate instead of invalid, as old clients would mostly GetAndUpdate
            // it wrong and see all their transactions rejected).
//			final String state = (validation[2].getRowCount() > 0) ? validation[2].fetchRow(0).getString(0) : "XX";

            if (output.hasNext()) {
                return output.next().getValues().get(0).getString();
            } else {
                return "XX";
            }

        } catch (DatabaseException | QueryPlanException e) {
            e.printStackTrace();
        }

        return null;//should not go here.
    }

    /**
     * Records a vote
     * <p>
     * "INSERT INTO votes (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
     *
     * @param voteId
     * @param phoneNumber
     * @param state
     * @param contestantNumber
     * @param timestamp
     */
    private void insertVoteStmt(SimpleDatabase.Transaction transaction, Long voteId, Long phoneNumber, String state, int contestantNumber, TimestampType timestamp) {

        List<DataBox> values = new ArrayList<>();
        values.add(new LongDataBox(voteId));
        values.add(new LongDataBox(phoneNumber));
        values.add(new StringDataBox(state, state.length()));
        values.add(new IntDataBox(contestantNumber));
        values.add(new TimeStampDataBox(timestamp));

        try {
            transaction.addRecord("votes", new SchemaRecord(values));

        } catch (DatabaseException e) {
            e.printStackTrace();
        }

    }

    private PhoneCallGenerator.PhoneCall getPhoneCall(Tuple in) {
        return (PhoneCallGenerator.PhoneCall) in.getValue(0);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {

        final PhoneCallGenerator.PhoneCall phoneCall = getPhoneCall(in);

        final long bid = in.getBID();
        final Long voteId = phoneCall.voteId;
        final Long phoneNumber = phoneCall.phoneNumber;
        int contestantNumber = phoneCall.contestantNumber;

//		SimpleDatabase.Transaction transaction = db.beginTransaction();

        //checkContestantStmt: it should not appear in the store yet.
//		checkContestant(transaction, contestantNumber);

        //checkVoterStmt
//		checkVoterStmt(transaction, phoneNumber);

        //checkStateStmt
//		final String state = checkStateStmt(transaction, (short) (phoneNumber / 10000000L));


        TimestampType timestamp = new TimestampType();
        long ts = System.currentTimeMillis() / 1000;

        // Post the vote
        //voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
//		insertVoteStmt(transaction, voteId, phoneNumber, "null", contestantNumber, timestamp);

//		transaction.end();

        // Send the vote downstream

        StreamValues value = new StreamValues(voteId, phoneNumber, state, contestantNumber, timestamp, ts);
        collector.emit(bid, value);

//
//        String word = input.getStringByField(Field.WORD, index);
//        MutableLong count = counts.computeIfAbsent(word, k -> new MutableLong(0));
//        count.increment();
//        StreamValues value_list = new StreamValues(word, count.longValue());
//
//        final GeneralMsg.marker marker = input.msg[index].getMarker();
//        if (marker != null) {
//            collector.emit_marked(value_list, marker);
//        } else
//            collector.emit(value_list);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
        LOG.info("PID  = " + pid);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.voteId, Field.phoneNumber, Field.state, Field.contestantNumber, Field.timestamp, Field.ts);
    }

}
