package common.util.math;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
/**
 * Author: Thilina
 * Date: 11/1/14
 */
public class SummaryArchive {
    private final List<Double> archive = new ArrayList<>();
    private final Long sliceLength;
    public SummaryArchive(Long sliceLength) {
        this.sliceLength = sliceLength;
    }
    public void archive(Double value) {
        archive.add(value);
    }
    public double getMedian() {
        int currentIndex = archive.size() - 1;
        int k = 1;
        int numberOfSlices = (int) (24 * 60 * 60 / sliceLength);
        int prev = (currentIndex + 2) - numberOfSlices * k;
        List<Double> values = new ArrayList<>();
        while (prev > 0) {
            values.add(archive.get(prev));
            k++;
            prev = (currentIndex + 2) - numberOfSlices * k;
        }
        return (values.size() > 0) ? getMedian(values) : 0.0;
    }
    private double getMedian(List<Double> values) {
        Collections.sort(values);
        int length = values.size();
        if (length % 2 == 0) {
            return (values.get(length / 2) + values.get(length / 2 + 1)) / 2;
        } else {
            return values.get(length / 2 + 1);
        }
    }
}