package common.topology.transactional.initializer;

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
import storage.datatype.DataBox;
import storage.datatype.DoubleDataBox;
import storage.datatype.LongDataBox;
import storage.datatype.HashSetDataBox;
import storage.datatype.StringDataBox;
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

    private void insertTweetRecord(long tID, long cID, String tValue, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("bookEntries", new TableRecord(TweetRecord(tID, cID, tValue), pid, spinlock_));
            else
                db.InsertRecord("bookEntries", new TableRecord(TweetRecord(tID, cID, tValue)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private void insertClusterRecord(long cID, double growthRate, int pid, SpinLock[] spinlock_) {
        try {
            if (spinlock_ != null)
                db.InsertRecord("bookEntries", new TableRecord(ClusterRecord(cID, growthRate), pid, spinlock_));
            else
                db.InsertRecord("bookEntries", new TableRecord(ClusterRecord(cID, growthRate)));
        } catch (DatabaseException e) {
            e.printStackTrace();
        }
    }

    private SchemaRecord TweetRecord(long tID, long cID, String tValue) {
        List<DataBox> values = new ArrayList<>();
        values.add(new LongDataBox(tID));
        values.add(new LongDataBox(cID));
        values.add(new StringDataBox(tValue, tValue.length()));
        return new SchemaRecord(values);
    }

    private SchemaRecord ClusterRecord(long cID, double growthRate) {
        List<DataBox> values = new ArrayList<>();
        values.add(new LongDataBox(cID));
        values.add(new DoubleDataBox(growthRate));
        return new SchemaRecord(values);
    }

    private RecordSchema TweetSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new StringDataBox());
        fieldNames.add("Tweet_ID");
        fieldNames.add("Cluster_ID");
        fieldNames.add("Tweet_Value");
        return new RecordSchema(fieldNames, dataBoxes);
    }

    private RecordSchema ClusterSchema() {
        List<DataBox> dataBoxes = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        dataBoxes.add(new LongDataBox());
        dataBoxes.add(new DoubleDataBox());
        fieldNames.add("Cluster_ID");
        fieldNames.add("Growth_Rate");
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
        RecordSchema tweet = TweetSchema();
        db.createTable(tweet, "tweet");
        RecordSchema cluster = ClusterSchema();
        db.createTable(cluster, "cluster");
    }
}
