package execution.runtime.collector.impl;

import com.google.common.util.concurrent.AtomicDouble;

/**
 * A singleton class
 */
public class BIDGenerator2 {
    private static final BIDGenerator2 ourInstance = new BIDGenerator2();
    private final AtomicDouble bid = new AtomicDouble();

    private BIDGenerator2() {
        bid.set(0);
    }

    public static BIDGenerator2 getInstance() {
        return ourInstance;
    }

    public double getAndIncrement() {
        return bid.addAndGet(1);
    }
}
