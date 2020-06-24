package sesame.components.windowing;

public class BasicEvent<T> implements Event {
    private final T event;
    private final long ts;

    public BasicEvent(T event, long ts) {
        this.event = event;
        this.ts = ts;
    }

    @Override
    public long getTimestamp() {
        return ts;
    }

    @Override
    public T get() {
        return event;
    }

    @Override
    public boolean isWatermark() {
        return false;
    }

    @Override
    public String toString() {
        return "BasicEvent{" +
                "input_event=" + event +
                ", ts=" + ts +
                '}';
    }
}
