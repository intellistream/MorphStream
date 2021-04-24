package transaction.function;
public class Condition {
    public final long arg1;
    public final long arg2;
    public final long arg3;
    public Condition(long arg1, long arg2, long arg3) {
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
    }
    public Condition(long arg1, long arg2) {
        this.arg1 = arg1;
        this.arg2 = arg2;
        arg3 = -1;
    }
}
