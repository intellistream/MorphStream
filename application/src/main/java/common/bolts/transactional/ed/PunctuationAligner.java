package common.bolts.transactional.ed;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.tweetWindowSize;
import static common.CONTROL.wordWindowSize;
import static common.CONTROL.clusterTableSize;

public class PunctuationAligner {
    public static AtomicInteger trEventCount = new AtomicInteger(0);
    public static AtomicInteger wuEventCount = new AtomicInteger(0);
    public static AtomicInteger tcEventCount = new AtomicInteger(0);
    public static AtomicInteger cuEventCount = new AtomicInteger(0);
    public static AtomicInteger esEventCount = new AtomicInteger(0);
    public static CyclicBarrier trBarrier = new CyclicBarrier(4);

}
