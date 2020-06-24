package sesame.components.windowing;
public class DefaultEvictionContext implements EvictionContext {
    private final Long referenceTime;
    private final Long currentCount;
    private final Long slidingCount;
    private final Long slidingInterval;
    public DefaultEvictionContext(Long referenceTime) {
        this(referenceTime, null);
    }
    private DefaultEvictionContext(Long referenceTime, Long currentCount) {
        this(referenceTime, currentCount, null);
    }
    private DefaultEvictionContext(Long referenceTime, Long currentCount, Long slidingCount) {
        this(referenceTime, currentCount, slidingCount, null);
    }
    public DefaultEvictionContext(Long referenceTime, Long currentCount, Long slidingCount, Long slidingInterval) {
        this.referenceTime = referenceTime;
        this.currentCount = currentCount;
        this.slidingCount = slidingCount;
        this.slidingInterval = slidingInterval;
    }
    @Override
    public Long getReferenceTime() {
        return referenceTime;
    }
    @Override
    public Long getCurrentCount() {
        return currentCount;
    }
    @Override
    public Long getSlidingCount() {
        return slidingCount;
    }
    @Override
    public Long getSlidingInterval() {
        return slidingInterval;
    }
}
