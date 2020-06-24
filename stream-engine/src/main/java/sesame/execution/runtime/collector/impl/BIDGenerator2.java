package sesame.execution.runtime.collector.impl;
import java.util.concurrent.atomic.AtomicLong;
/**
 * A singleton class
 */
public class BIDGenerator2 {
    private static BIDGenerator2 ourInstance = new BIDGenerator2();
    private final AtomicLong bid = new AtomicLong();
    private BIDGenerator2() {
        bid.set(0);
    }
    public static BIDGenerator2 getInstance() {
        return ourInstance;
    }
    public long getAndIncrement() {
        return bid.getAndIncrement();
    }
}
