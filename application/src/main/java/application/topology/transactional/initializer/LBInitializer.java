package application.topology.transactional.initializer;

import application.util.Configuration;
import state_engine.Database;
import state_engine.DatabaseException;
import state_engine.SimpleDatabase;
import state_engine.common.SpinLock;
import state_engine.query.QueryPlan;
import state_engine.query.QueryPlanException;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.*;
import state_engine.storage.table.RecordSchema;
import state_engine.transaction.TableInitilizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LBInitializer extends TableInitilizer {

    // Domain data: matching lists of Area codes and States
    private static final short[] areaCodes = new short[]{
            907, 205, 256, 334, 251, 870, 501, 479, 480, 602, 623, 928, 520, 341, 764, 628, 831, 925,
            909, 562, 661, 510, 650, 949, 760, 415, 951, 209, 669, 408, 559, 626, 442, 530, 916, 627,
            714, 707, 310, 323, 213, 424, 747, 818, 858, 935, 619, 805, 369, 720, 303, 970, 719, 860,
            203, 959, 475, 202, 302, 689, 407, 239, 850, 727, 321, 754, 954, 927, 352, 863, 386, 904,
            561, 772, 786, 305, 941, 813, 478, 770, 470, 404, 762, 706, 678, 912, 229, 808, 515, 319,
            563, 641, 712, 208, 217, 872, 312, 773, 464, 708, 224, 847, 779, 815, 618, 309, 331, 630,
            317, 765, 574, 260, 219, 812, 913, 785, 316, 620, 606, 859, 502, 270, 504, 985, 225, 318,
            337, 774, 508, 339, 781, 857, 617, 978, 351, 413, 443, 410, 301, 240, 207, 517, 810, 278,
            679, 313, 586, 947, 248, 734, 269, 989, 906, 616, 231, 612, 320, 651, 763, 952, 218, 507,
            636, 660, 975, 816, 573, 314, 557, 417, 769, 601, 662, 228, 406, 336, 252, 984, 919, 980,
            910, 828, 704, 701, 402, 308, 603, 908, 848, 732, 551, 201, 862, 973, 609, 856, 575, 957,
            505, 775, 702, 315, 518, 646, 347, 212, 718, 516, 917, 845, 631, 716, 585, 607, 914, 216,
            330, 234, 567, 419, 440, 380, 740, 614, 283, 513, 937, 918, 580, 405, 503, 541, 971, 814,
            717, 570, 878, 835, 484, 610, 267, 215, 724, 412, 401, 843, 864, 803, 605, 423, 865, 931,
            615, 901, 731, 254, 325, 713, 940, 817, 430, 903, 806, 737, 512, 361, 210, 979, 936, 409,
            972, 469, 214, 682, 832, 281, 830, 956, 432, 915, 435, 801, 385, 434, 804, 757, 703, 571,
            276, 236, 540, 802, 509, 360, 564, 206, 425, 253, 715, 920, 262, 414, 608, 304, 307};
    private static final String[] states = new String[]{
            "AK", "AL", "AL", "AL", "AL", "AR", "AR", "AR", "AZ", "AZ", "AZ", "AZ", "AZ", "CA", "CA",
            "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA",
            "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA", "CA",
            "CA", "CA", "CA", "CA", "CO", "CO", "CO", "CO", "SL", "SL", "SL", "SL", "DC", "DE", "FL",
            "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL", "FL",
            "FL", "FL", "FL", "GA", "GA", "GA", "GA", "GA", "GA", "GA", "GA", "GA", "HI", "IA", "IA",
            "IA", "IA", "IA", "ID", "IL", "IL", "IL", "IL", "IL", "IL", "IL", "IL", "IL", "IL", "IL",
            "IL", "IL", "IL", "IN", "IN", "IN", "IN", "IN", "IN", "KS", "KS", "KS", "KS", "KY", "KY",
            "KY", "KY", "LA", "LA", "LA", "LA", "LA", "MA", "MA", "MA", "MA", "MA", "MA", "MA", "MA",
            "MA", "MD", "MD", "MD", "MD", "ME", "MI", "MI", "MI", "MI", "MI", "MI", "MI", "MI", "MI",
            "MI", "MI", "MI", "MI", "MI", "MN", "MN", "MN", "MN", "MN", "MN", "MN", "MO", "MO", "MO",
            "MO", "MO", "MO", "MO", "MO", "MS", "MS", "MS", "MS", "MT", "NC", "NC", "NC", "NC", "NC",
            "NC", "NC", "NC", "ND", "NE", "NE", "NH", "NJ", "NJ", "NJ", "NJ", "NJ", "NJ", "NJ", "NJ",
            "NJ", "NM", "NM", "NM", "NV", "NV", "NY", "NY", "NY", "NY", "NY", "NY", "NY", "NY", "NY",
            "NY", "NY", "NY", "NY", "NY", "OH", "OH", "OH", "OH", "OH", "OH", "OH", "OH", "OH", "OH",
            "OH", "OH", "OK", "OK", "OK", "OR", "OR", "OR", "PA", "PA", "PA", "PA", "PA", "PA", "PA",
            "PA", "PA", "PA", "PA", "RI", "SC", "SC", "SC", "SD", "TN", "TN", "TN", "TN", "TN", "TN",
            "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX",
            "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "TX", "UT", "UT", "UT", "VA", "VA",
            "VA", "VA", "VA", "VA", "VA", "VA", "VT", "WA", "WA", "WA", "WA", "WA", "WA", "WI", "WI",
            "WI", "WI", "WI", "WV", "WY"};
    private final Database db;


    private final int contestant_name_length = 50;
    private final int state_length = 4;

    public LBInitializer(Database db, double scale_factor, double theta, int tthread, Configuration config) {
        super(db, scale_factor, theta, tthread, config);
        this.db = db;
    }

    /**
     * "SELECT COUNT(*) FROM contestants;"
     *
     * @param transaction
     */
    private long checkContestant(SimpleDatabase.Transaction transaction) {
        try {
            QueryPlan queryPlan = transaction.query("contestants");
            queryPlan.count();
            final Iterator<SchemaRecord> output = queryPlan.execute();

            if (output.hasNext()) {
                final int existingContestantCount = output.next().getValues().get(0).getInt();
                // if the data is initialized, return the contestant count
                if (existingContestantCount != 0) {
                    return existingContestantCount;
                }
            }
        } catch (DatabaseException | QueryPlanException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * "INSERT INTO contestants (contestant_name, contestant_number) VALUES (?, ?);");
     *
     * @param transaction
     * @param contestant_name
     * @param contestant_number
     */
    private void insertContestantStmt(SimpleDatabase.Transaction transaction, int contestant_number, String contestant_name) {

        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(contestant_number));
        values.add(new StringDataBox(contestant_name, contestant_name_length));


        try {
            transaction.addRecord("contestants", new SchemaRecord(values));

        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * "INSERT INTO area_code_state VALUES (?,?);"
     *
     * @param transaction
     * @param areaCode
     * @param state
     */
    private void insertACSStmt(SimpleDatabase.Transaction transaction, short areaCode, String state) {

        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(areaCode));
        values.add(new StringDataBox(state, state_length));

        try {
            transaction.addRecord("area_code_state", new SchemaRecord(values));

        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }


    /**
     * "INSERT INTO votes_count (row_id, cnt) VALUES (1, 0);"
     *
     * @param transaction
     */
    private void insertVoteCountStmt(SimpleDatabase.Transaction transaction) {

        List<DataBox> values = new ArrayList<>();
        values.add(new IntDataBox(1));
        values.add(new IntDataBox(0));


        try {
            transaction.addRecord("votes_count", new SchemaRecord(values));

        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    /**
     * this.getClientHandle().callProcedure("Initialize",
     * numContestants,
     * VoterSStoreExampleConstants.CONTESTANT_NAMES_CSV);
     *
     * @param maxContestants
     * @param contestants
     */
    @Override
    public void loadDB(int maxContestants, String contestants) {

        String[] contestantArray = contestants.split(",");

        //  voltQueueSQL(checkStmt);
//		checkContestant(transaction);

        //insertVoteCountStmt
//		insertVoteCountStmt( );

        //insertContestantStmt
        for (int i = 0; i < maxContestants; i++) {
//			insertContestantStmt( , i + 1, contestantArray[i]);
        }
        //insertACSStmt
        for (int i = 0; i < areaCodes.length; i++) {
//			insertACSStmt( , areaCodes[i], states[i]);
        }

    }


    /**
     * -- contestants table holds the contestants numbers (for voting) and names
     * CREATE TABLE contestants
     * (
     * contestant_number integer     NOT NULL
     * , contestant_name   varchar(50) NOT NULL
     * , CONSTRAINT PK_contestants PRIMARY DEVICE_ID
     * (
     * contestant_number
     * )
     * );
     */

    private RecordSchema contestantSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new StringDataBox(contestant_name_length));

        fieldNames.add("contestant_number");//PK
        fieldNames.add("contestant_name");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    private void contestantsTable() {
        RecordSchema s = contestantSchema();
        db.createTable(s, "contestants");
    }


    /**
     * -- Map of Area Codes and States for geolocation classification of incoming calls
     * CREATE TABLE area_code_state
     * (
     * area_code smallint   NOT NULL
     * , state     varchar(2) NOT NULL
     * , CONSTRAINT PK_area_code_state PRIMARY DEVICE_ID
     * (
     * area_code
     * )
     * );
     */

    private RecordSchema area_code_stateSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new StringDataBox(state_length));

        fieldNames.add("area_code");//PK
        fieldNames.add("state");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    private void area_code_stateTable() {
        RecordSchema s = area_code_stateSchema();
        db.createTable(s, "area_code_state");
    }

    /**
     * -- votes table holds every valid vote.
     * --   VoterSStoreExamples are not allowed to submit more than <x> votes, x is passed to client application
     * CREATE TABLE votes
     * (
     * vote_id            bigint     NOT NULL,
     * phone_number       bigint     NOT NULL
     * , state              varchar(2) NOT NULL -- REFERENCES area_code_state (state)
     * , contestant_number  integer    NOT NULL REFERENCES contestants (contestant_number)
     * , created	     timestamp  NOT NULL
     * , CONSTRAINT PK_votes PRIMARY DEVICE_ID
     * (
     * vote_id
     * )
     * -- PARTITION BY ( phone_number )
     * );
     *
     * @return
     */
    private RecordSchema votesSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new StringDataBox(state_length));
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new TimeStampDataBox());

        fieldNames.add("vote_id");//PK
        fieldNames.add("phone_number");
        fieldNames.add("state");
        fieldNames.add("contestant_number");
        fieldNames.add("created");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    /**
     * -- votes table holds every valid vote.
     */
    private void votesTable() {
        RecordSchema s = votesSchema();
        db.createTable(s, "votes");
    }


    /**
     * ?
     *
     * @return
     */
    private RecordSchema votes_countSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();

        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new IntDataBox());

        fieldNames.add("row_id");//PK
        fieldNames.add("cnt");

        return new RecordSchema(fieldNames, dataBoxes);
    }

    /**
     * ?
     */
    private void votes_countTable() {
        RecordSchema s = votes_countSchema();
        db.createTable(s, "votes_count");
    }

    public void creates_Table(Configuration config) {
        votes_countTable();
        contestantsTable();
        area_code_stateTable();
        votesTable();
    }

    @Override
    public void loadDB(int thread_id, int NUMTasks) {

    }

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUMTasks) {

    }


    @Override
    public void loadData_Central(double scale_factor, double theta, int partition_interval, SpinLock[] spinlock_) {

    }

    @Override
    public void loadData_Central(double scale_factor, double theta) {

    }

    @Override
    public boolean Prepared(String file) {
        return false;
    }

    @Override
    public void store(String file_path) {

    }

    @Override
    public Object create_new_event(int number_partitions, int bid) {
        return null;
    }
}
