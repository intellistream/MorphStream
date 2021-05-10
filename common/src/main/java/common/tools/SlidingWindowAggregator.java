package common.tools;
import java.io.Serializable;
import java.util.Map;
/**
 * Created by szhang026 on 8/10/2015.
 */
public class SlidingWindowAggregator<T> implements Serializable {
    private static final long serialVersionUID = -2645063988768785810L;
    private final SlotBasedCounter<T> objCounter;
    private int headSlot;
    private int tailSlot;
    private final int windowLengthInSlots;
    public SlidingWindowAggregator(int windowLengthInSlots) {
        if (windowLengthInSlots < 2) {
            throw new IllegalArgumentException(
                    "Window length in slots must be at least two (you requested "
                            + windowLengthInSlots + ")");
        }
        this.windowLengthInSlots = windowLengthInSlots;
        this.objCounter = new SlotBasedCounter<>(this.windowLengthInSlots);
        this.headSlot = 0;
        this.tailSlot = slotAfter(headSlot);
    }
    public void incrementCount(T obj) {
        objCounter.incrementCount(obj, headSlot);
    }
    public void incrementCount(T obj, long increment) {
        objCounter.incrementCount(obj, headSlot, increment);
    }
    /**
     * Return the current (total) counts of all tracked objects, then advance the window.
     * <p/>
     * Whenever this method is called, we consider the counts of the current sliding window to be available to and
     * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
     * objects within the next "chunk" of the sliding window.
     *
     * @return The current (total) counts of all tracked objects.
     */
    public Map<T, Long> getCountsThenAdvanceWindow() {
        Map<T, Long> counts = objCounter.getCounts();
        objCounter.wipeZeros();
        objCounter.wipeSlot(tailSlot);
        advanceHead();
        return counts;
    }
    private void advanceHead() {
        headSlot = tailSlot;
        tailSlot = slotAfter(tailSlot);
    }
    private int slotAfter(int slot) {
        return (slot + 1) % windowLengthInSlots;
    }
}
