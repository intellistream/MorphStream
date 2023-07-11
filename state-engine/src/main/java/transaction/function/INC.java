package transaction.function;

public class INC extends Function {
    public INC(long delta) {
        this.delta_long = delta;
    }

    public INC(long delta, Condition condition) {
        this.delta_long = delta;
    }

    @Override
    public String toString() {
        return String.valueOf(delta_long);
    }
}
