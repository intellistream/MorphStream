package engine.stream.components.windowing;

/**
 * Watermark input_event used for tracking progress of time when
 * processing input_event based ts.
 */
public class WaterMarkEvent<T> extends BasicEvent<T> {
    public WaterMarkEvent(long ts) {
        super(null, ts);
    }

    @Override
    public boolean isWatermark() {
        return true;
    }

    @Override
    public String toString() {
        return "WaterMarkEvent{} " + super.toString();
    }
}
