package util.model.spam;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.Serializable;

public class Word implements Serializable {
    private static final long serialVersionUID = 1667802979041340740L;
    private String word;    // The String itself
    private int countBad;   // The total times it appears in "bad" messages 
    private int countGood;  // The total times it appears in "good" messages
    private float rBad;     // bad count / total bad words
    private float rGood;    // good count / total good words
    private float pSpam;    // probability this word is Spam

    public Word() {
    }

    // Create a word, initialize all vars to 0
    public Word(String s) {
        word = s;
        countBad = 0;
        countGood = 0;
        rBad = 0.0f;
        rGood = 0.0f;
        pSpam = 0.0f;
    }

    private Word(String word, int countBad, int countGood, float rBad, float rGood, float pSpam) {
        this.word = word;
        this.countBad = countBad;
        this.countGood = countGood;
        this.rBad = rBad;
        this.rGood = rGood;
        this.pSpam = pSpam;
    }

    // Increment bad counter
    public void countBad() {
        countBad++;
    }

    // Increment good counter
    public void countGood() {
        countGood++;
    }

    public void countBad(int increment) {
        countBad += increment;
    }

    // Increment good counter
    public void countGood(int increment) {
        countGood += increment;
    }

    public void calcProbs(long badTotal, long goodTotal) {
        calcBadProb(badTotal);
        calcGoodProb(goodTotal);
        finalizeProb();
    }

    // Computer how often this word is bad
    public void calcBadProb(long total) {
        if (total > 0)
            rBad = countBad / (float) total;
    }

    // Computer how often this word is good
    public void calcGoodProb(long total) {
        if (total > 0)
            rGood = 2 * countGood / (float) total; // multiply 2 to help fight against false positives (via Graham)
    }

    // Implement bayes rules to computer how likely this word is "spam"
    public void finalizeProb() {
        if (rGood + rBad > 0)
            pSpam = rBad / (rBad + rGood);
        if (pSpam < 0.01f)
            pSpam = 0.01f;
        else if (pSpam > 0.99f)
            pSpam = 0.99f;
    }

    // The "interesting" rating for a word is
    // How different from 0.5 it is
    public float interesting() {
        return Math.abs(0.5f - pSpam);
    }

    // Some getters and setters
    public float getPGood() {
        return rGood;
    }

    public float getPBad() {
        return rBad;
    }

    public float getPSpam() {
        return pSpam;
    }

    public void setPSpam(float f) {
        pSpam = f;
    }

    public String getWord() {
        return word;
    }

    @Override
    public String toString() {
        return "Word{" + "word=" + word + ", countBad=" + countBad + ", countGood=" + countGood + ", rBad=" + rBad + ", rGood=" + rGood + ", pSpam=" + pSpam + '}';
    }

    public static class WordSerializer extends Serializer<Word> {
        @Override
        public void write(Kryo kryo, Output output, Word object) {
            output.writeString(object.word);
            output.writeInt(object.countBad);
            output.writeInt(object.countGood);
            output.writeFloat(object.rBad);
            output.writeFloat(object.rGood);
            output.writeFloat(object.pSpam);
        }

        @Override
        public Word read(Kryo kryo, Input input, Class<Word> type) {
            return new Word(input.readString(), input.readInt(), input.readInt(),
                    input.readFloat(), input.readFloat(), input.readFloat());
        }
    }
}