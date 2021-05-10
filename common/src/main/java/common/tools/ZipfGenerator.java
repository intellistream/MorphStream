package common.tools;
import java.util.Random;
// Based on http://diveintodata.org/tag/zipf/
public class ZipfGenerator {
    private final Random rnd = new Random(0);
    private final int size;
    private final double skew;
    private double bottom = 0;
    public ZipfGenerator(int size, double skew) {
        this.size = size;
        this.skew = skew;
        for (int i = 1; i <= size; i++) {
            this.bottom += (1 / Math.pow(i, this.skew));
        }
    }
    // the next() method returns an random rank id.
    // The frequency of returned rank ids are follows Zipf distribution.
    public int next() {
        int rank;
        double friquency = 0;
        double dice;
        rank = rnd.nextInt(size) + 1;
        friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
        dice = rnd.nextDouble();
        while (!(dice < friquency)) {
            rank = rnd.nextInt(size) + 1;
            friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
            dice = rnd.nextDouble();
        }
        return rank;
    }
    // This method returns a probability that the given rank occurs.
    public double getProbability(int rank) {
        return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
    }
}
