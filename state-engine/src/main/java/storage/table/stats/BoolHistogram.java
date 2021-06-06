package storage.table.stats;

import operator.QueryPlan.PredicateOperator;
import storage.datatype.DataBox;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class BoolHistogram implements Histogram<Boolean> {
    private final boolean estimate;
    private final List<Bucket<Boolean>> buckets;
    private int numDistinct;

    public BoolHistogram() {
        this.estimate = false;
        this.buckets = new ArrayList<>();
        this.buckets.add(new Bucket<>(true));
        this.buckets.add(new Bucket<>(false));
    }

    public BoolHistogram(List<Bucket<Boolean>> buckets, int numDistinct) {
        this.numDistinct = numDistinct;
        this.estimate = true;
        this.buckets = buckets;
    }

    public BoolHistogram copyWithReduction(float reductionFactor) {
        List<Bucket<Boolean>> copyBuckets = new ArrayList<>();
        int trueCount = (int) (this.buckets.get(0).getCount() * reductionFactor);
        int falseCount = (int) (this.buckets.get(1).getCount() * reductionFactor);
        copyBuckets.add(new Bucket<>(true));
        copyBuckets.get(0).increment(trueCount);
        copyBuckets.add(new Bucket<>(false));
        copyBuckets.get(1).increment(falseCount);
        return new BoolHistogram(copyBuckets, this.getNumDistinct());
    }

    public BoolHistogram copyWithPredicate(PredicateOperator predicate,
                                           DataBox value) {
        List<Bucket<Boolean>> copyBuckets = new ArrayList<>();
        copyBuckets.add(new Bucket<>(true));
        copyBuckets.add(new Bucket<>(false));
        int trueCount = this.buckets.get(0).getCount();
        int falseCount = this.buckets.get(1).getCount();
        boolean predValue = value.getBool();
        switch (predicate) {
            case EQUALS:
                if (predValue == true) {
                    copyBuckets.get(0).increment(trueCount);
                } else {
                    copyBuckets.get(1).increment(falseCount);
                }
                break;
            case NOT_EQUALS:
                if (predValue == true) {
                    copyBuckets.get(1).increment(falseCount);
                } else {
                    copyBuckets.get(0).increment(trueCount);
                }
                break;
            default:
                break;
        }
        int numDistinct = 0;
        if (copyBuckets.get(0).getCount() > 0) {
            numDistinct += 1;
        }
        if (copyBuckets.get(1).getCount() > 0) {
            numDistinct += 1;
        }
        return new BoolHistogram(copyBuckets, numDistinct);
    }

    @Override
    public int getSampleCount() {
        return 0;
    }

    public float computeReductionFactor(PredicateOperator predicate,
                                        DataBox value) {
        float reductionFactor = 1;
        boolean predValue = value.getBool();
        int trueCount = this.buckets.get(0).getCount();
        int falseCount = this.buckets.get(1).getCount();
        float totalCount = trueCount + falseCount;
        switch (predicate) {
            case EQUALS:
                if (predValue == true) {
                    reductionFactor = trueCount / totalCount;
                } else {
                    reductionFactor = falseCount / totalCount;
                }
                break;
            case NOT_EQUALS:
                if (predValue == true) {
                    reductionFactor = falseCount / totalCount;
                } else {
                    reductionFactor = trueCount / totalCount;
                }
                break;
            default:
                break;
        }
        return reductionFactor;
    }

    public List<Bucket<Boolean>> getAllBuckets() {
        return this.buckets;
    }

    public void removeValue(Boolean value) {
        if (value) {
            this.buckets.get(0).decrement();
        } else {
            this.buckets.get(1).decrement();
        }
    }

    @Override
    public Long get(Boolean value) {
        return null;
    }

    @Override
    public Collection<Boolean> values() {
        return null;
    }

    public void add(Boolean value) {
        if (value) {
            this.buckets.get(0).increment();
        } else {
            this.buckets.get(1).increment();
        }
    }

    public int getEntriesInRange(Boolean start, Boolean end) {
        int entries = 0;
        if (start) {
            entries = this.buckets.get(0).getCount();
        } else {
            if (end) {
                entries += this.buckets.get(0).getCount();
            }
            entries += this.buckets.get(1).getCount();
        }
        return entries;
    }

    public int getMinValueIndex() {
        throw new UnsupportedOperationException();
    }

    public int getMaxValueIndex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean getMinValue() {
        return null;
    }

    @Override
    public Boolean getMaxValue() {
        return null;
    }

    public int getNumDistinct() {
        int numDistinct = 0;
        if (this.buckets.get(0).getCount() > 0) {
            numDistinct += 1;
        }
        if (this.buckets.get(1).getCount() > 0) {
            numDistinct += 1;
        }
        return numDistinct;
    }
}