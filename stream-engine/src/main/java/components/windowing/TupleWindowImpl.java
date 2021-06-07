package components.windowing;

import execution.runtime.tuple.impl.Tuple;

import java.util.List;

/**
 * Holds the expired, new and current tuples in a window.
 */
public class TupleWindowImpl implements TupleWindow {
    private final List<Tuple> tuples;
    private final List<Tuple> newTuples;
    private final List<Tuple> expiredTuples;
    private final Long startTimestamp;
    private final Long endTimestamp;

    public TupleWindowImpl(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples) {
        this(tuples, newTuples, expiredTuples, null, null);
    }

    public TupleWindowImpl(List<Tuple> tuples, List<Tuple> newTuples, List<Tuple> expiredTuples,
                           Long startTimestamp, Long endTimestamp) {
        this.tuples = tuples;
        this.newTuples = newTuples;
        this.expiredTuples = expiredTuples;
        this.startTimestamp = startTimestamp;
        this.endTimestamp = endTimestamp;
    }

    @Override
    public List<Tuple> get() {
        return tuples;
    }

    @Override
    public List<Tuple> getNew() {
        return newTuples;
    }

    @Override
    public List<Tuple> getExpired() {
        return expiredTuples;
    }

    @Override
    public Long getStartTimestamp() {
        return startTimestamp;
    }

    @Override
    public Long getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public String toString() {
        return "TupleWindowImpl{" +
                "tuples=" + tuples +
                ", newTuples=" + newTuples +
                ", expiredTuples=" + expiredTuples +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TupleWindowImpl that = (TupleWindowImpl) o;
        return (tuples != null ? tuples.equals(that.tuples) : that.tuples == null) && (newTuples != null ? newTuples.equals(that.newTuples) : that.newTuples == null) && (expiredTuples != null ? expiredTuples.equals(that.expiredTuples) : that.expiredTuples == null);
    }

    @Override
    public int hashCode() {
        int result = tuples != null ? tuples.hashCode() : 0;
        result = 31 * result + (newTuples != null ? newTuples.hashCode() : 0);
        result = 31 * result + (expiredTuples != null ? expiredTuples.hashCode() : 0);
        return result;
    }
}