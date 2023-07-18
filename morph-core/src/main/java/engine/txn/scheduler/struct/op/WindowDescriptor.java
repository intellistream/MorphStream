package engine.txn.scheduler.struct.op;

public class WindowDescriptor {
    private final boolean isWindowed;
    private final long range;

    public WindowDescriptor(boolean isWindowed, long range) {
        this.isWindowed = isWindowed;
        this.range = range;
    }

    public boolean isWindowed() {
        return isWindowed;
    }


    public long getRange() {
        return range;
    }
}
