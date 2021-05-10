package common.util.model.spam;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * @author mayconbordin
 */
public class OfflineTraining {
    private static final Logger LOG = LoggerFactory.getLogger(OfflineTraining.class.getSimpleName());
    private static final String splitregex = "\\W";
    private static final Pattern wordregex = Pattern.compile("\\w+");
    private static Kryo kryoInstance;
    protected WordMap words;
    public OfflineTraining() {
        words = new WordMap();
    }
    private static Kryo getKryoInstance() {
        if (kryoInstance == null) {
            kryoInstance = new Kryo();
            kryoInstance.register(Word.class, new Word.WordSerializer());
            kryoInstance.register(WordMap.class, new WordMap.WordMapSerializer());
        }
        return kryoInstance;
    }
    private static void printUsage() {
        System.out.print(
                "Usage: java -cp <jar-file> OfflineTraining <command>\n"
                        + "Commands:\n"
                        + "  train <input-path> <output-path>\n"
                        + "  check <wordmap-path>\n"
        );
    }
    public static void main(String[] args) throws IOException {
        System.out.println(Arrays.toString(args));
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }
        if (args[0].equals("check")) {
            if (args.length < 2) {
                printUsage();
                System.exit(1);
            }
            OfflineTraining filter = new OfflineTraining();
            filter.loadTraining(args[1]);
            LOG.info("Num words: {}", filter.words.values().size());
        } else if (args[0].equals("train")) {
            if (args.length < 3) {
                printUsage();
                System.exit(1);
            }
            String inputPath = args[1];
            String outputPath = args[2];
            OfflineTraining filter = new OfflineTraining();
            List<String> trainingSet = Files.readLines(new File(inputPath + "/index"), Charset.defaultCharset());
            LOG.info("Number of emails: {}", trainingSet.size());
            for (int i = 0; i < trainingSet.size(); i++) {
                if (i % 1000 == 0) {
                    LOG.info("Training set {}", i);
                }
                String[] train = trainingSet.get(i).split("\\s+");
                boolean isSpam = train[0].toLowerCase().trim().equals("spam");
                String content = Files.toString(new File(inputPath + "/data/" + train[1]), Charset.defaultCharset());
                filter.train(content, isSpam);
            }
            filter.finalizeTraining();
            boolean result = filter.saveTraining(outputPath);
            if (!result) {
                LOG.error("Training object not saved.");
            } else {
                LOG.info("Training object saved!");
            }
        } else {
            printUsage();
            System.exit(1);
        }
    }
    public void train(String content, boolean isSpam) {
        String[] tokens = content.split(splitregex);
        for (String token : tokens) {
            String word = token.toLowerCase();
            Matcher m = wordregex.matcher(word);
            if (m.matches()) {
                Word w;
                if (words.containsKey(word)) {
                    w = words.get(word);
                } else {
                    w = new Word(word);
                    words.put(word, w);
                }
                if (isSpam) {
                    w.countBad();
                    words.incSpamTotal(1);
                } else {
                    w.countGood();
                    words.incHamTotal(1);
                }
            }
        }
    }
    public void finalizeTraining() {
        for (Word word : words.values()) {
            word.calcBadProb(words.getSpamTotal());
            word.calcGoodProb(words.getHamTotal());
            word.finalizeProb();
        }
    }
    public boolean saveTraining(String filePath) {
        try {
            Output output = new Output(new FileOutputStream(filePath));
            getKryoInstance().writeObject(output, words);
            output.close();
            return true;
        } catch (FileNotFoundException ex) {
            LOG.error("The output file path was not found", ex);
        } catch (KryoException ex) {
            LOG.error("Serialization error", ex);
        }
        return false;
    }
    public boolean loadTraining(String filePath) {
        try {
            Input input = new Input(new FileInputStream(filePath));
            WordMap object = getKryoInstance().readObject(input, WordMap.class);
            input.close();
            words = object;
            return true;
        } catch (FileNotFoundException ex) {
            LOG.error("The input file path was not found", ex);
        } catch (KryoException ex) {
            LOG.error("Deserialization error", ex);
        }
        return false;
    }
}