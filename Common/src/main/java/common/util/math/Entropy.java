package common.util.math;
import java.util.List;
import java.util.Map;
public class Entropy {
    public static double calculateEntropyHistogram(int totalCount, Map<Double, List<String>> histogram) {
        double entropy = 0.0;
        for (Map.Entry<Double, List<String>> entry : histogram.entrySet()) {
            double prob = (double) entry.getValue().size() / totalCount;
            entropy -= prob * log2(prob);
        }
        return entropy;
    }
    public static double calculateEntropyNormalDistribution(double sigma) {
        if (sigma == 0) {
            sigma = 0.000001;
        }
        return 0.5 * Math.log(2 * Math.PI * Math.E * Math.pow(sigma, 2));
    }
    private static double log2(double value) {
        return Math.log(value) / Math.log(2);
    }
}