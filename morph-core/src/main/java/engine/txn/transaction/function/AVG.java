package engine.txn.transaction.function;

public class AVG extends Function {
    public AVG(Integer delta) {
        this.delta_double = delta;
    }

    @Override
    public String toString() {
        return String.valueOf(delta_double);
    }
}
