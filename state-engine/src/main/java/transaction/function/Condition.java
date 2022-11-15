package transaction.function;

public class Condition {
    public final long arg1;
    public final long arg2;
    public final long arg3;

    public final String stringArg1;

    public Condition(long arg1, long arg2, long arg3) {
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.arg3 = arg3;
        stringArg1 = "";
    }

    public Condition(long arg1, long arg2) {
        this.arg1 = arg1;
        this.arg2 = arg2;
        arg3 = -1;
        stringArg1 = "";
    }

    public Condition(long arg1) {
        this.arg1 = arg1;
        arg2 = -1;
        arg3 = -1;
        stringArg1 = "";
    }

    public Condition(long arg1, String stringArg1) {
        this.arg1 = arg1;
        arg2 = -1;
        arg3 = -1;
        this.stringArg1 = stringArg1;
    }

    public Condition() {
        arg1 = -1;
        arg2 = -1;
        arg3 = -1;
        stringArg1 = "";
    }
}
