package engine.txn.transaction.function;

public class CNT extends Function {
    public CNT(int delta) {
        this.delta_int = delta;
    }

    @Override
    public String toString() {
        return String.valueOf(delta_int);
    }
}
