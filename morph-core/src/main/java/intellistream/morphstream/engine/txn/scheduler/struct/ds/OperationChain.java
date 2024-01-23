package intellistream.morphstream.engine.txn.scheduler.struct.ds;

public class OperationChain implements Comparable<OperationChain> {

    @Override
    public int compareTo(OperationChain o) {
        if (o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }
}
