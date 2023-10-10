package intellistream.morphstream.engine.txn.transaction.function;

public class SUM extends Function {
    public SUM() {
    }

    public SUM(long delta) {
        this.delta_long = delta;
    }
}
