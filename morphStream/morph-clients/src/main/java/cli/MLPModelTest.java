package cli;

import intellistream.morphstream.transNFV.adaptation.PerformanceModel;
import org.apache.commons.math.stat.descriptive.SynchronizedDescriptiveStatistics;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class MLPModelTest {
    public static void main(String[] args) throws JAXBException, IOException, SAXException {
        SynchronizedDescriptiveStatistics latencyInNS = new SynchronizedDescriptiveStatistics();

        double[] keySkew = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};
        double workloadSkew = 0;
        double[] readRatio = {0, 10, 20, 30, 40, 50, 60, 70, 80, 90};
        double locality = 10;
        double scopeRatio = 0;

        PerformanceModel.loadModel();

        for (double skew : keySkew) {
            for (double read : readRatio) {
                long startTime = System.nanoTime();
                String prediction = PerformanceModel.predictOptimalStrategy(skew, workloadSkew, read, locality, scopeRatio);
                long latency = System.nanoTime() - startTime;
                latencyInNS.addValue(latency);
                System.out.println("Predicted optimal strategy: " + prediction);
            }
        }

        System.out.println("Average latency: " + latencyInNS.getMean() + " ns");
    }
}
