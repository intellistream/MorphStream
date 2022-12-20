package common.topology.transactional.initializer;

import benchmark.DataHolder;
import benchmark.datagenerator.DataGenerator;
import benchmark.datagenerator.DataGeneratorConfig;
import benchmark.datagenerator.apps.ED.TPGTxnGenerator.EDTPGDataGenerator;
import benchmark.datagenerator.apps.ED.TPGTxnGenerator.EDTPGDataGeneratorConfig;
import benchmark.datagenerator.apps.ED.TPGTxnGenerator.EDTPGDynamicDataGenerator;
import benchmark.dynamicWorkloadGenerator.DynamicDataGeneratorConfig;
import common.collections.Configuration;
import common.collections.OsUtils;
import common.param.TxnEvent;
import common.param.ed.tr.TREvent;
import db.Database;
import db.DatabaseException;
import javafx.application.Application;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scheduler.context.SchedulerContext;
import storage.SchemaRecord;
import storage.TableRecord;
import storage.datatype.*;
import storage.table.RecordSchema;
import transaction.TableInitilizer;
import utils.AppConfig;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

import static common.CONTROL.enable_log;
import static common.CONTROL.enable_states_partition;
import static common.Constants.Event_Path;
import static profiler.Metrics.NUM_ITEMS;
import static transaction.State.configure_store;

public class EDInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(EDInitializer.class);

    private final int numberOfStates;
    private final int startingValue = 10000;
    private final DataGeneratorConfig dataConfig;
    private final int partitionOffset;
    private final int NUM_ACCESS;
    private final int Transaction_Length;
    //different R-W ratio.
    //just enable one of the decision array
    protected transient boolean[] read_decision;
    int i = 0;
    private String dataRootPath;
    private DataGenerator dataGenerator;

    public EDInitializer(Database db, int numberOfStates, double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
        floor_interval = (int) Math.floor(numberOfStates / (double) tthread);//NUM_ITEMS / tthread;
        this.dataRootPath = config.getString("rootFilePath");
        this.partitionOffset = numberOfStates / tthread;
        this.NUM_ACCESS = config.getInt("NUM_ACCESS");
        this.Transaction_Length = config.getInt("Transaction_Length");
        this.numberOfStates = numberOfStates;
        // set up generator
        configure_store(theta, tthread, numberOfStates);
        createTPGGenerator(config);
        dataConfig = dataGenerator.getDataConfig();
    }

    protected void createTPGGenerator(Configuration config) {
        if (config.getBoolean("isDynamic")) {
            DynamicDataGeneratorConfig dynamicDataGeneratorConfig = new DynamicDataGeneratorConfig();
            dynamicDataGeneratorConfig.initialize(config);
            configurePath(dynamicDataGeneratorConfig);
            dataGenerator = new EDTPGDynamicDataGenerator(dynamicDataGeneratorConfig);
        } else {
            EDTPGDataGeneratorConfig dataConfig = new EDTPGDataGeneratorConfig();
            dataConfig.initialize(config);
            configurePath(dataConfig);
            dataGenerator = new EDTPGDataGenerator(dataConfig);
        }
    }

    /**
     * Control the input file path.
     * TODO: think carefully which configuration shall vary.
     *
     * @param dataConfig
     */

    private void configurePath(DataGeneratorConfig dataConfig) {
        MessageDigest digest;
        String subFolder = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes;
            if (dataConfig instanceof EDTPGDataGeneratorConfig)
                bytes = digest.digest(String.format("%d_%d_%d_%s_%s",
                                dataConfig.getTotalThreads(),
                                dataConfig.getTotalEvents(),
                                dataConfig.getnKeyStates(),
                                AppConfig.isCyclic,
                                config.getString("workloadType"))
                        .getBytes(StandardCharsets.UTF_8));
            else if (dataConfig instanceof DynamicDataGeneratorConfig)
                bytes = digest.digest(String.format("%d_%d_%d_%s_%s",
                                dataConfig.getTotalThreads(),
                                dataConfig.getTotalEvents(),
                                dataConfig.getnKeyStates(),
                                AppConfig.isCyclic,
                                config.getString("workloadType"))
                        .getBytes(StandardCharsets.UTF_8));
            else
                bytes = digest.digest(String.format("%d_%d_%d",
                                dataConfig.getTotalThreads(),
                                dataConfig.getTotalEvents(),
                                dataConfig.getnKeyStates())
                        .getBytes(StandardCharsets.UTF_8));

            subFolder = OsUtils.osWrapperPostFix(
                    DatatypeConverter.printHexBinary(bytes));
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataConfig.setRootPath(dataConfig.getRootPath() + OsUtils.OS_wrapper(subFolder));
        dataConfig.setIdsPath(dataConfig.getIdsPath() + OsUtils.OS_wrapper(subFolder));
        this.dataRootPath += OsUtils.OS_wrapper(subFolder);
    }

    @Override
    public boolean Generate() {
        String folder = dataRootPath;
        File file = new File(folder);
        if (file.exists()) {
            if (config.getBoolean("isDynamic")) {
                //file.delete();
                dataGenerator.generateTPGProperties();
            }
            if (enable_log) LOG.info("Data already exists.. skipping data generation...");
            return false;
        }
        file.mkdirs();

        dataGenerator.generateStream();//prepare input events.
        if (enable_log) LOG.info(String.format("Data Generator will dump data at %s.", dataRootPath));
        dataGenerator.dumpGeneratedDataToFile();
        if (enable_log) LOG.info("Data Generation is done...");
        dataGenerator.clearDataStructures();
        return true;
    }

    @Override
    protected void Load() throws IOException {
        int totalEvents = dataConfig.getTotalEvents();
        boolean shufflingActive = dataConfig.getShufflingActive();
        String folder = dataConfig.getRootPath();
        File file = new File(folder + "events.txt");
        int[] p_bids = new int[tthread];
        if (file.exists()) {
            if (enable_log) LOG.info("Reading tweet registrant events...");
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file))); //Put data file into reader
            loadTREvents(reader, totalEvents, shufflingActive, p_bids); //Pass the reader to method
            reader.close();
        }
    }

    private void loadTREvents(BufferedReader reader, int totalEvents, boolean shufflingActive, int[] p_bids) throws IOException {
        String txn = reader.readLine();
        int count = 0;
//        int p_bids[] = new int[tthread];
        while (txn != null) {
            String[] split = txn.split(",");
            int npid = (int) (Long.parseLong(split[1]) / partitionOffset);
            count++;

            // Construct bid array
            HashMap<Integer, Integer> pids = new HashMap<>();
            for (int i = 1; i < 5; i++) {
                pids.put((int) (Long.parseLong(String.valueOf(split[i].hashCode())) / partitionOffset), 0); //TODO: Set pid as 0 for all input words
            }

            //Construct String[] words from readLine()
            String[] words = new String[3]; //TODO: Hard-coded number of words in tweet: 3
            System.arraycopy(split, 2, words, 0, 3);

            // Construct TR Event
            TREvent event = new TREvent(
                    Integer.parseInt(split[0]), //bid
                    npid, //pid
                    Arrays.toString(p_bids), //bid_arrary
                    Arrays.toString(pids.keySet().toArray(new Integer[0])), // partition_index
                    2,//num_of_partition TODO: Hard-coded number of arguments in TR Event
                    split[1], //tweetID
                    words //String[] words
            );

            DataHolder.events.add(event);
            if (enable_log) LOG.debug(String.format("%d deposit read...", count));
            txn = reader.readLine();
        }
        if (enable_log) LOG.info("Done reading transfer events...");
        if (shufflingActive) {
            shuffleEvents(DataHolder.events, totalEvents);
        }
    }

    private void shuffleEvents(ArrayList<TxnEvent> txnEvents, int totalEvents) {
        Random random = new Random();
        int index;
        TxnEvent temp;
        for (int i = totalEvents - 1; i > 0; i--) {
            index = random.nextInt(i + 1);
            temp = txnEvents.get(index);
            txnEvents.set(index, txnEvents.get(i));
            txnEvents.set(i, temp);
        }
    }

    @Override
    public void store(String file_name) throws IOException {
        double ratio_of_multi_partition = config.getDouble("ratio_of_multi_partition", 1);
        this.number_partitions = Math.min(tthread, config.getInt("number_partitions"));
        double ratio_of_read = config.getDouble("ratio_of_read", 0.5);
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + enable_states_partition)
                + OsUtils.OS_wrapper("NUM_EVENTS=" + config.getInt("totalEvents"))
                + OsUtils.OS_wrapper("ratio_of_multi_partition=" + ratio_of_multi_partition)
                + OsUtils.OS_wrapper("number_partitions=" + number_partitions)
                + OsUtils.OS_wrapper("ratio_of_read=" + ratio_of_read)
                + OsUtils.OS_wrapper("NUM_ACCESS=" + NUM_ACCESS)
                + OsUtils.OS_wrapper("theta=" + theta)
                + OsUtils.OS_wrapper("NUM_ITEMS=" + NUM_ITEMS);
        File file = new File(event_path);
        file.mkdirs(); // If the directory containing the file and/or its parent(s) does not exist
        BufferedWriter w;
        w = new BufferedWriter(new FileWriter(new File(event_path + OsUtils.OS_wrapper(file_name))));
        for (Object event : db.getEventManager().input_events) {
            TREvent trEvent = (TREvent) event;
            String sb =
                    trEvent.getBid() +//0 -- bid
                            split_exp +
                            trEvent.getPid() +//1
                            split_exp +
                            Arrays.toString(trEvent.getBid_array()) +//2
                            split_exp +
                            trEvent.num_p() +//3 num of p
                            split_exp +
                            "TREvent" +//4 input_event types.
                            split_exp +
                            trEvent.getTweetID() +//5 tweet ID
                            split_exp +
                            Arrays.toString(trEvent.getWords()) //6 words
                    ;
            w.write(sb
                    + "\n");
        }
        w.close();
    }


    @Override
    public void loadDB(int thread_id, int NUM_TASK) {
        loadDB(thread_id, null, NUM_TASK);
    }


    //TODO: This initialize table with some default records.
    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUM_TASK) {
        int partition_interval = (int) Math.ceil(numberOfStates / (double) NUM_TASK);
        int left_bound = thread_id * partition_interval;
        int right_bound;
        if (thread_id == NUM_TASK - 1) {//last executor need to handle left-over
            right_bound = config.getInt("NUM_ITEMS");
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        int pid;
        String _key;
        for (int key = left_bound; key < right_bound; key++) {
            pid = get_pid(partition_interval, key);
            _key = String.valueOf(key);

            //This initializes table with some default records.
            String[] wordList = {"word1", "word2", "word3"};
            insertTweetRecord(_key, wordList, pid, spinlock);
        }
        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }


    @Override
    public void loadDB(SchedulerContext context, int thread_id, int NUM_TASK) {
        loadDB(context, thread_id, null, NUM_TASK);
    }

    /**
     * TODO: code clean up to deduplicate.
     *
     * @param context
     * @param thread_id
     * @param spinlock
     * @param NUM_TASK
     */
    @Override
    public void loadDB(SchedulerContext context, int thread_id, SpinLock[] spinlock, int NUM_TASK) {
        int partition_interval = (int) Math.ceil(config.getInt("NUM_ITEMS") / (double) NUM_TASK);
        int left_bound = thread_id * partition_interval;
        int right_bound;
        int tweet_word_count = 3;
        if (thread_id == NUM_TASK - 1) {//last executor need to handle left-over
            right_bound = config.getInt("NUM_ITEMS");
        } else {
            right_bound = (thread_id + 1) * partition_interval;
        }
        int pid;
        String _key;

        //Initialize tweet table
        for (int key = left_bound; key < right_bound; key++) {
            pid = get_pid(partition_interval, key);
            _key = String.valueOf(key);
            String[] wordList = {};
            insertTweetRecord(_key, wordList, pid, spinlock);
        }

        LOG.info("Inserted tweet record from row : " + left_bound + " to " + right_bound);

        int word_left_bound = tweet_word_count * left_bound;
        int word_right_bound = tweet_word_count * right_bound;
        //Initialize word table
        for (int key = word_left_bound; key < word_right_bound; key++) { //assume each tweet has 3 words
            pid = get_pid(partition_interval, key);
            _key = String.valueOf(key);
            String wordValue = "";
            String[] tweetList = {};
            int countOccurWindow = -1;
            double tfIdf = -1;
            int lastOccurWindow = -1;
            int frequency = -1;
            boolean isBurst = false;
            insertWordRecord(_key, wordValue, tweetList, countOccurWindow, tfIdf, lastOccurWindow, frequency, isBurst, pid, spinlock);
        }

        LOG.info("Inserted word record from row : " + word_left_bound + " to " + word_right_bound);

        //Initialize cluster table
        for (int key = left_bound; key < right_bound; key++) {
            pid = get_pid(partition_interval, key);
            _key = String.valueOf(key);
            String[] wordList = {};
            int countNewTweet = -1;
            int clusterSize = -1;
            boolean isEvent = false;
            insertClusterRecord(_key, wordList, countNewTweet, clusterSize, isEvent, pid, spinlock);
        }

        LOG.info("Inserted cluster record from row : " + left_bound + " to " + right_bound);

        if (enable_log)
            LOG.info("Thread:" + thread_id + " finished loading data from: " + left_bound + " to: " + right_bound);
    }

    private void insertTweetRecord(String tweetID, String[] wordList, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("tweet_table", new TableRecord(TweetRecord(tweetID, wordList), pid, spinlock_));
            else
                db.InsertRecord("tweet_table", new TableRecord(TweetRecord(tweetID, wordList)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertWordRecord(String wordID, String wordValue, String[] tweetList, long countOccurWindow, double tfIdf,
                                  int lastOccurWindow, long frequency, boolean isBurst, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("word_table", new TableRecord(WordRecord(wordID, wordValue, tweetList, countOccurWindow,
                        tfIdf, lastOccurWindow, frequency, isBurst), pid, spinlock_));
            else
                db.InsertRecord("word_table", new TableRecord(WordRecord(wordID, wordValue, tweetList, countOccurWindow,
                        tfIdf, lastOccurWindow, frequency, isBurst)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertClusterRecord(String clusterID, String[] wordList, int countNewTweet, int clusterSize, boolean isEvent,
                                     int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("tweet_table", new TableRecord(ClusterRecord(clusterID, wordList, countNewTweet, clusterSize, isEvent), pid, spinlock_));
            else
                db.InsertRecord("tweet_table", new TableRecord(ClusterRecord(clusterID, wordList, countNewTweet, clusterSize, isEvent)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private RecordSchema WordSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());       //Primary key
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new ListStringDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new DoubleDataBox());
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new BoolDataBox());
        fieldNames.add("Word_ID"); // 0
        fieldNames.add("Word_Value"); // 1
        fieldNames.add("Tweet_List"); // 2
        fieldNames.add("Count_Occur_Window"); // 3
        fieldNames.add("TF_IDF"); // 4
        fieldNames.add("Last_Occur_Window"); // 5
        fieldNames.add("Frequency"); // 6
        fieldNames.add("Is_Burst"); // 7
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private SchemaRecord WordRecord(String wordID, String wordValue, String[] tweetList, long countOccurWindow, double tfIdf,
                                    int lastOccurWindow, long frequency, boolean isBurst) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(wordID, wordID.length()));
        values.add(new StringDataBox(wordValue));
        values.add(new ListStringDataBox(tweetList));
        values.add(new LongDataBox(countOccurWindow));
        values.add(new DoubleDataBox(tfIdf));
        values.add(new IntDataBox(lastOccurWindow));
        values.add(new LongDataBox(frequency));
        values.add(new BoolDataBox(isBurst));
        return new SchemaRecord(values);
    }

    private RecordSchema TweetSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new ListStringDataBox());
        fieldNames.add("Tweet_ID"); // 0
        fieldNames.add("Word_List"); // 1

        return new RecordSchema(fieldNames, dataBoxes);
    }

    private SchemaRecord TweetRecord(String tweetID, String[] wordList) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(tweetID, tweetID.length()));
        values.add(new ListStringDataBox(wordList));
        return new SchemaRecord(values);
    }

    private RecordSchema ClusterSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new ListStringDataBox());
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new BoolDataBox());
        fieldNames.add("Cluster_ID"); // 0
        fieldNames.add("Word_List"); // 1
        fieldNames.add("Count_New_Tweet"); // 2
        fieldNames.add("Cluster_Size"); // 3
        fieldNames.add("Is_Event"); // 4
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private SchemaRecord ClusterRecord(String clusterID, String[] wordList, int countNewTweet, int clusterSize, boolean isEvent) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(clusterID, clusterID.length()));
        values.add(new ListStringDataBox(wordList));
        values.add(new IntDataBox(countNewTweet));
        values.add(new IntDataBox(clusterSize));
        values.add(new BoolDataBox(isEvent));
        return new SchemaRecord(values);
    }

    @Override
    public List<String> getTranToDecisionConf() {
        return dataGenerator.getTranToDecisionConf();
    }

    //Done
    public void creates_Table(Configuration config) {
        RecordSchema word = WordSchema();
        db.createTable(word, "word_table");
        RecordSchema tweet = TweetSchema();
        db.createTable(tweet, "tweet_table");
        RecordSchema cluster = ClusterSchema();
        db.createTable(cluster, "cluster_table");

        try {
            prepare_input_events(config.getInt("totalEvents"));
            if (getTranToDecisionConf() != null && getTranToDecisionConf().size() != 0) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String decision : getTranToDecisionConf()) {
                    stringBuilder.append(decision);
                    stringBuilder.append(";");
                }
                stringBuilder.deleteCharAt(stringBuilder.length() - 1);
                config.put("WorkloadConfig", stringBuilder.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //TODO: Place pre-processing methods outside of system
    //Pre-processing: Read tweet csv file and convert to normalized String tokens
    public List<String> readTweetTokens(String PATH, String delim) {
        List<String> tokens = new ArrayList<>();
        List<String> stopWords = Arrays.asList("I", "a", "the"); // TODO: Use a better way to eliminate stopwords
        String newLine = "";
        StringTokenizer tokenizer;
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Objects.requireNonNull(Application.class.getResourceAsStream(
                        "/" + PATH))))) {
            while ((newLine = br.readLine()) != null) {
                tokenizer = new StringTokenizer(newLine, delim);
                while (tokenizer.hasMoreElements()) {
                    if (!stopWords.contains(tokenizer.nextToken())) {
                        tokens.add(normalizeWord(tokenizer.nextToken()));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tokens;
    }

    // Normalize word with repeating characters to only two consecutive characters
    // Remove all invalid characters but alphabet letters and numbers
    // Remove all extra spaces (is this necessary?)
    public String normalizeWord(String word) {
        word = word.replaceAll("[^a-zA-Z0-9]+", "");
        String regex = "([a-z])\\1{2,}";
        word = word.replaceAll(regex, "$1$1");
        word = word.trim().replaceAll(" +", " ");
        return word;
    }
}
