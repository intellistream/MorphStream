package util.math;

import java.util.List;
import java.util.Map;

public class MaximumLikelihoodNormalDistribution {
    private double mu;
    private double sigma;

    public MaximumLikelihoodNormalDistribution(int totalCount, Map<Double, List<String>> histogram) {
        mu = 0;
        sigma = 0;
        if (totalCount == 0) {
            return;
        }
        for (Map.Entry<Double, List<String>> entry : histogram.entrySet()) {
            mu += entry.getKey() * entry.getValue().size();
        }
        mu /= totalCount;
        for (Map.Entry<Double, List<String>> entry : histogram.entrySet()) {
            double value = Math.pow((entry.getKey() - mu), 2);
            sigma += value * entry.getValue().size();
            // System.out.println("(" + entry.getKey() + " - " + mu + ")^2 = " + value);
        }
        sigma = sigma / (totalCount - 1);
        // sigma = totalCount / (totalCount - 1) * sigma;
        sigma = Math.sqrt(sigma);
    }

    public double getMu() {
        return mu;
    }

    public double getSigma() {
        return sigma;
    }
}