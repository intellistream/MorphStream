package intellistream.morphstream.common.io.Rdma.RdmaUtils.Stats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RdmaRemoteFetchHistogram {
    private final int numBuckets;
    private final int bucketSize;
    private final List<AtomicInteger> results = new ArrayList<>();
    public RdmaRemoteFetchHistogram(int numBuckets, int bucketSize) {
        this.numBuckets = numBuckets;
        this.bucketSize = bucketSize;
        for (int i = 0; i < numBuckets; i++) {
            results.add(new AtomicInteger(0));
        }
    }
    public void addSample(int sampleInMs) {
        assert sampleInMs >= 0;
        int bucket = sampleInMs / bucketSize;
        if (bucket < this.numBuckets) {
            results.get(bucket).incrementAndGet();
        } else { // sample is larger than the largest bucket
            results.get(numBuckets).incrementAndGet();
        }
    }
    public String getString() {
        String str = "";
        for (int i = 0; i < numBuckets; i++) {
            str += ((i * bucketSize) + "-" + ((i + 1) * bucketSize) + "ms " + results.get(i) + ", ");
        }
        str += ((numBuckets * bucketSize) + "-...ms " + results.get(numBuckets));
        return str;
    }
}
