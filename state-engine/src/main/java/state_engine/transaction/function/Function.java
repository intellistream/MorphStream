package state_engine.transaction.function;
/**
 * Push down function
 */
public abstract class Function {
    public int delta_int;
    public long delta_long;
    public double delta_double;
    public double[] new_value;
}
