package common.topology.transactional.initializer;

import benchmark.datagenerator.DataGeneratorConfig;
import common.collections.Configuration;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import static common.CONTROL.enable_log;
import static common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static utils.PartitionHelper.getPartition_interval;

public class EDInitializer extends TableInitilizer {
    private static final Logger LOG = LoggerFactory.getLogger(TPInitializer.class);

    public EDInitializer(Database db, double theta, int tthread, Configuration config) {
        super(db, theta, tthread, config);
    }

    //TODO: Implement this
    protected void createTPGGenerator(Configuration config) {}

    /**
     * Control the input file path.
     * TODO: think carefully which configuration shall vary.
     *
     * @param dataConfig
     */
    private void configurePath(DataGeneratorConfig dataConfig) {}

    @Override
    public void loadDB(int thread_id, SpinLock[] spinlock, int NUMTasks) {
        //
    }

    @Override
    public void loadDB(SchedulerContext context, int thread_id, int NUMTasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadDB(SchedulerContext context, int thread_id, SpinLock[] spinlock, int NUMTasks) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadDB(int thread_id, int NUMTasks) {
        //
    }

    //TODO: Implement this
    private void insertTweetRecord(long tID, long cID, String tValue, int pid, SpinLock[] spinlock_) {
//        try {
//            if (spinlock_ != null)
//                db.InsertRecord("bookEntries", new TableRecord(TweetRecord(tID, cID, tValue), pid, spinlock_));
//            else
//                db.InsertRecord("bookEntries", new TableRecord(TweetRecord(tID, cID, tValue)));
//        } catch (DatabaseException e) {
//            e.printStackTrace();
//        }
    }

    //TODO: Implement this
    private void insertClusterRecord(long cID, double growthRate, int pid, SpinLock[] spinlock_) {
//        try {
//            if (spinlock_ != null)
//                db.InsertRecord("bookEntries", new TableRecord(ClusterRecord(cID, growthRate), pid, spinlock_));
//            else
//                db.InsertRecord("bookEntries", new TableRecord(ClusterRecord(cID, growthRate)));
//        } catch (DatabaseException e) {
//            e.printStackTrace();
//        }
    }

    //TODO: Implement HashMapDataBox datatype
    private SchemaRecord GlobalWordRecord(long wordID, String wordValue, int countOccurWindow, double tfIdf, int lastWindow, HashSet windowMap) {
        List<DataBox> values = new ArrayList<>();
        values.add(new LongDataBox(wordID));
        values.add(new StringDataBox(wordValue));
        values.add(new IntDataBox(countOccurWindow));
        values.add(new DoubleDataBox(tfIdf));
        values.add(new IntDataBox(lastWindow));
        values.add(new HashSetDataBox(windowMap));
        return new SchemaRecord(values);
    }

    private SchemaRecord LocalWordRecord(long wordID, String wordValue, HashSet tweetMap, int frequency, boolean isBurst) {
        List<DataBox> values = new ArrayList<>();
        values.add(new LongDataBox(wordID));
        values.add(new StringDataBox(wordValue));
        values.add(new HashSetDataBox(tweetMap));
        values.add(new IntDataBox(frequency));
        values.add(new BoolDataBox(isBurst));
        return new SchemaRecord(values);
    }

    private SchemaRecord LocalTweetRecord(long tweetID, String tweetValue, long clusterID, HashSet wordMap) {
        List<DataBox> values = new ArrayList<>();
        values.add(new LongDataBox(tweetID));
        values.add(new StringDataBox(tweetValue));
        values.add(new LongDataBox(clusterID));
        values.add(new HashSetDataBox(wordMap));
        return new SchemaRecord(values);
    }

    private SchemaRecord ClusterRecord(long clusterID, HashSet tweetList, long aliveTime, long countNewTweet) {
        List<DataBox> values = new ArrayList<>();
        values.add(new LongDataBox(clusterID));
        values.add(new HashSetDataBox(tweetList));
        values.add(new LongDataBox(aliveTime));
        values.add(new LongDataBox(countNewTweet));
        return new SchemaRecord(values);
    }

    private RecordSchema GlobalWordSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new DoubleDataBox());
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new HashSetDataBox());
        fieldNames.add("Word_ID");
        fieldNames.add("Word_Value");
        fieldNames.add("Count_Occur_Window");
        fieldNames.add("TF_IDF");
        fieldNames.add("Last_Window");
        fieldNames.add("Window_Map");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema LocalWordTable() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new HashSetDataBox());
        dataBoxes.add(new IntDataBox());
        dataBoxes.add(new BoolDataBox());
        fieldNames.add("Word_ID");
        fieldNames.add("Word_Value");
        fieldNames.add("Tweet_Map");
        fieldNames.add("Frequency");
        fieldNames.add("Is_Burst");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema LocalTweetSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new StringDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new HashSetDataBox());
        fieldNames.add("Tweet_ID");
        fieldNames.add("Tweet_Value");
        fieldNames.add("Cluster_ID");
        fieldNames.add("Word_Map");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema ClusterSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new HashSetDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new LongDataBox());
        fieldNames.add("Cluster_ID");
        fieldNames.add("Tweet_List");
        fieldNames.add("Alive_Time");
        fieldNames.add("Count_New_Tweet");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    // Pre-processing: Read tweet csv file and convert to normalized String tokens
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
        word = word.replaceAll("[^a-zA-Z0-9]+","");
        String regex = "([a-z])\\1{2,}";
        word = word.replaceAll(regex, "$1$1");
        word = word.trim().replaceAll(" +", " ");
        return word;
    }

    @Override
    public boolean Generate() {
        return true;
    }

    @Override
    protected void Load() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void store(String file_name) {
    }

    @Override
    public List<String> getTranToDecisionConf() {
        return null;
    }

    public void creates_Table(Configuration config) {
        RecordSchema globalWord = GlobalWordSchema();
        db.createTable(globalWord, "global_word_table");
        RecordSchema localWord = LocalWordTable();
        db.createTable(localWord, "local_word_table");
        RecordSchema localTweet = LocalTweetSchema();
        db.createTable(localTweet, "local_tweet_table");
        RecordSchema cluster = ClusterSchema();
        db.createTable(cluster, "cluster_table");

        //TODO: Check this
        try {
            prepare_input_events(config.getInt("totalEvents"));
            if (getTranToDecisionConf() != null && getTranToDecisionConf().size() != 0){
                StringBuilder stringBuilder = new StringBuilder();
                for(String decision:getTranToDecisionConf()){
                    stringBuilder.append(decision);
                    stringBuilder.append(";");
                }
                stringBuilder.deleteCharAt(stringBuilder.length()-1);
                config.put("WorkloadConfig",stringBuilder.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
