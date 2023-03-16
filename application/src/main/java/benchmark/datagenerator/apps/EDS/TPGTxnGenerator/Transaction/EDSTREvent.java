package benchmark.datagenerator.apps.EDS.TPGTxnGenerator.Transaction;
import benchmark.datagenerator.Event;


public class EDSTREvent extends Event {
    private final int id;
    private final int tweetID;
    private final String[] words;

    public EDSTREvent(int id, int tweetID, String[] words) {
        this.id = id;
        this.tweetID = tweetID;
        this.words = words;
    }

    public int getTweetId() {
        return tweetID;
    }

    public String[] getWords() {
        return words;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        str.append(",").append(tweetID);
        for (String word : words) {
            str.append(",").append(word);
        }
        return str.toString();
    }

}
