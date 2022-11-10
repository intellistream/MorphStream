package benchmark.datagenerator.apps.ED.TPGTxnGenerator.Transaction;
import benchmark.datagenerator.Event;


public class EDTREvent extends Event {
    private final int id;
    private final int tweetID;
    private final String[] words;

    public EDTREvent(int id, int tweetID, String[] words) {
        this.id = id;
        this.tweetID = tweetID;
        this.words = words;
    }

    public int getTweetId() {
        return tweetID;
    }


    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        for (String word : words) {
            str.append(",").append(word);
        }
        return str.toString();
    }

}
