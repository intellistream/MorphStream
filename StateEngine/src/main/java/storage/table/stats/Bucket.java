package storage.table.stats;
/**
 * A parametrized type that represents a partition histogram bucket. Stores values of a certain type.
 *
 * @param <T> the type of values in this bucket
 */
public class Bucket<T> {
    private T start;
    private T end;
    private int count;
    public Bucket(T start) {
        this.start = start;
        this.end = null;
        this.count = 0;
    }
    public Bucket(T start, T end) {
        this.start = start;
        this.end = end;
        this.count = 0;
    }
    public T getStart() {
        return this.start;
    }
    public T getEnd() {
        return this.end;
    }
    public int getCount() {
        return this.count;
    }
    public void increment() {
        this.count++;
    }
    public void decrement() {
        this.count--;
    }
    public void increment(int addNum) {
        this.count += addNum;
    }
    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Bucket)) {
            return false;
        }
        Bucket otherBucket = (Bucket) other;
        return start.equals(otherBucket.start) && end.equals(otherBucket.end);
    }
}
